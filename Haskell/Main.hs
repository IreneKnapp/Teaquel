{-# LANGUAGE OverloadedStrings, FlexibleInstances, MultiParamTypeClasses #-}
module Main (main) where

import Control.Concurrent.MVar.Lifted
import Control.Monad.Base
import Control.Monad.Error
import Control.Monad.Reader
import Data.Array.IO
import Data.Char
import Data.List
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word
import Prelude hiding (FilePath)
import System.IO hiding (FilePath)


class ShowText item where
  showText :: item -> Text
class ShowTextList item where
  showTextList :: [item] -> Text


data Keyword
  = AtKeyword
  | AttachKeyword
  | CreateKeyword
  | DatabaseKeyword
  | DatabasesKeyword
  | DetachKeyword
  | DropKeyword
  | SchemaKeyword
  | ShowKeyword
  deriving (Eq)
instance ShowText Keyword where
  showText AtKeyword = "AT"
  showText AttachKeyword = "ATTACH"
  showText CreateKeyword = "CREATE"
  showText DetachKeyword = "DETACH"
  showText DatabaseKeyword = "DATABASE"
  showText DatabasesKeyword = "DATABASES"
  showText DropKeyword = "DROP"
  showText SchemaKeyword = "SCHEMA"
  showText ShowKeyword = "SHOW"


data Token
  = KeywordToken Keyword
  | LeftParenthesisToken
  | RightParenthesisToken
  | CommaToken
  | SemicolonToken
  | PeriodToken
  | NameToken Text
  | StringToken Text
instance Eq Token where
  KeywordToken a == KeywordToken b = a == b
  NameToken a == NameToken b = T.toLower a == T.toLower b
  StringToken a == StringToken b = a == b
  LeftParenthesisToken == LeftParenthesisToken = True
  RightParenthesisToken == RightParenthesisToken = True
  CommaToken == CommaToken = True
  SemicolonToken == SemicolonToken = True
  PeriodToken == PeriodToken = True
  _ == _ = False
instance ShowText Token where
  showText (KeywordToken keyword) = showText keyword
  showText LeftParenthesisToken = "("
  showText RightParenthesisToken = ")"
  showText CommaToken = ","
  showText SemicolonToken = ";"
  showText PeriodToken = "."
  showText (NameToken text) = text
  showText (StringToken text) =
    T.concat ["'",
              T.foldl' (\soFar c ->
                          T.concat [soFar, if c == '\'' then "''" else T.pack [c]])
                       ""
                       text,
              "'"]
instance ShowTextList Token where
  showTextList [] = ""
  showTextList [token] = showText token
  showTextList (tokenA:rest@(tokenB:_)) =
    let sharedTokenLexicalType =
          if tokenLexicalType tokenA == tokenLexicalType tokenB
            then Just $ tokenLexicalType tokenA
            else Nothing
        spaceAfterThenWord =
          tokenSpaceAfter tokenA &&
          (tokenLexicalType tokenB == WordTokenLexicalType)
        wordThenSpaceBefore =
          (tokenLexicalType tokenA == WordTokenLexicalType)
          && tokenSpaceBefore tokenB
        spaceBetween =
          spaceAfterThenWord
          || wordThenSpaceBefore
          || (case sharedTokenLexicalType of
                Nothing -> False
                Just PunctuationTokenLexicalType -> False
                Just _ -> True)
    in T.concat [showText tokenA,
                 if spaceBetween
                   then " "
                   else "",
                 showTextList rest]


data TokenLexicalType
  = WordTokenLexicalType
  | PunctuationTokenLexicalType
  | OperatorTokenLexicalType
  deriving (Eq, Ord)


tokenLexicalType :: Token -> TokenLexicalType
tokenLexicalType (KeywordToken _) = WordTokenLexicalType
tokenLexicalType (NameToken _) = WordTokenLexicalType
tokenLexicalType (StringToken _) = WordTokenLexicalType
tokenLexicalType LeftParenthesisToken = PunctuationTokenLexicalType
tokenLexicalType RightParenthesisToken = PunctuationTokenLexicalType
tokenLexicalType CommaToken = PunctuationTokenLexicalType
tokenLexicalType SemicolonToken = PunctuationTokenLexicalType
tokenLexicalType PeriodToken = PunctuationTokenLexicalType


tokenSpaceBefore :: Token -> Bool
tokenSpaceBefore LeftParenthesisToken = True
tokenSpaceBefore _ = False


tokenSpaceAfter :: Token -> Bool
tokenSpaceAfter RightParenthesisToken = True
tokenSpaceAfter CommaToken = True
tokenSpaceAfter SemicolonToken = True
tokenSpaceAfter _ = False


lexToken :: Text -> Maybe (Token, Text)
lexToken text =
  case T.uncons text of
    Nothing -> Nothing
    Just (character, rest)
      | isSpace character -> lexToken rest
      | isLetter character || character == '_' ->
        let (word, rest) =
              T.span (\c -> isLetter c || isDigit c || c == '_') text
            maybeKeyword = case T.unpack $ T.toUpper word of
                             "AT" -> Just AtKeyword
                             "ATTACH" -> Just AttachKeyword
                             "DETACH" -> Just DetachKeyword
                             "CREATE" -> Just CreateKeyword
                             "DATABASE" -> Just DatabaseKeyword
                             "DATABASES" -> Just DatabasesKeyword
                             "DROP" -> Just DropKeyword
                             "SCHEMA" -> Just SchemaKeyword
                             "SHOW" -> Just ShowKeyword
                             _ -> Nothing
        in case maybeKeyword of
             Just keyword -> Just (KeywordToken keyword, rest)
             Nothing -> Just (NameToken word, rest)
      | character == '\'' ->
        let loop soFar rest
              | T.length rest == 0 =
                  Nothing
              | T.head rest /= '\'' =
                  loop (T.snoc soFar $ T.head rest) (T.tail rest)
              | T.length rest == 1 =
                  Just (StringToken soFar, T.tail rest)
              | T.head (T.tail rest) /= '\'' =
                  Just (StringToken soFar, T.tail rest)
              | otherwise =
                  loop (T.snoc soFar '\'') (T.tail $ T.tail rest)
        in loop "" rest
      | otherwise ->
        let maybeToken = case character of
                           '(' -> Just LeftParenthesisToken
                           ')' -> Just RightParenthesisToken
                           ',' -> Just CommaToken
                           ';' -> Just SemicolonToken
                           _ -> Nothing
        in fmap (\token -> (token, rest)) maybeToken


lexTokens :: Text -> Maybe [Token]
lexTokens text = do
  let loop soFar text = do
        if T.null $ T.dropWhile isSpace text
          then Just soFar
          else do
            (token, rest) <- lexToken text
            loop (soFar ++ [token]) rest
  loop [] text


class ShowTokens showable where
  showTokens :: showable -> [Token]
class ParseTokens parseable where
  parseTokens :: [Token] -> Maybe (parseable, [Token])


data DatabaseName = DatabaseName Text
instance ShowTokens DatabaseName where
  showTokens (DatabaseName name) =
    [NameToken name]
instance ParseTokens DatabaseName where
  parseTokens (NameToken name
               : rest) =
    Just (DatabaseName name, rest)
  parseTokens _ = Nothing


data SchemaName = SchemaName (Maybe DatabaseName) Text
instance ShowTokens SchemaName where
  showTokens (SchemaName Nothing name) =
    [NameToken name]
  showTokens (SchemaName (Just databaseName) name) =
    concat [showTokens databaseName,
            [PeriodToken,
             NameToken name]]
instance ParseTokens SchemaName where
  parseTokens (NameToken databaseName
               : PeriodToken
               : NameToken schemaName
               : rest) =
    Just (SchemaName (Just $ DatabaseName databaseName) schemaName, rest)
  parseTokens (NameToken schemaName
               : rest) =
    Just (SchemaName Nothing schemaName, rest)
  parseTokens _ = Nothing


data FilePath = FilePath Text
instance ShowTokens FilePath where
  showTokens (FilePath string) =
    [StringToken string]
instance ParseTokens FilePath where
  parseTokens (StringToken filePath
               : rest) =
    Just (FilePath filePath, rest)
  parseTokens _ = Nothing


data Statement
  = AttachDatabaseStatement DatabaseName FilePath
  | DetachDatabaseStatement DatabaseName
  | ShowDatabasesStatement
  | CreateSchemaStatement SchemaName
  | DropSchemaStatement SchemaName
instance ShowTokens Statement where
  showTokens (AttachDatabaseStatement name filePath) =
    concat [[KeywordToken AttachKeyword,
             KeywordToken DatabaseKeyword],
            showTokens name,
            [KeywordToken AtKeyword],
            showTokens filePath,
            [SemicolonToken]]
  showTokens (DetachDatabaseStatement name) =
    concat [[KeywordToken AttachKeyword,
             KeywordToken DatabaseKeyword],
            showTokens name,
            [SemicolonToken]]
  showTokens (ShowDatabasesStatement) =
    [KeywordToken ShowKeyword,
     KeywordToken DatabasesKeyword,
     SemicolonToken]
  showTokens (CreateSchemaStatement name) =
    concat [[KeywordToken CreateKeyword,
             KeywordToken SchemaKeyword],
            showTokens name,
            [SemicolonToken]]
  showTokens (DropSchemaStatement name) =
    concat [[KeywordToken DropKeyword,
             KeywordToken SchemaKeyword],
            showTokens name,
            [SemicolonToken]]
instance ParseTokens Statement where
  parseTokens (KeywordToken AttachKeyword
               : KeywordToken DatabaseKeyword
               : rest) =
    case parseTokens rest of
      Just (databaseName,
            (KeywordToken AtKeyword
             : rest)) ->
        case parseTokens rest of
          Just (filePath, rest) ->
            case rest of
              (SemicolonToken
               : rest) ->
                Just (AttachDatabaseStatement databaseName filePath, rest)
              _ -> Nothing
          _ -> Nothing
      _ -> Nothing
  parseTokens (KeywordToken DetachKeyword
               : KeywordToken DatabaseKeyword
               : rest) =
    case parseTokens rest of
      Just (databaseName, rest) ->
        case rest of
          (SemicolonToken
           : rest) ->
            Just (DetachDatabaseStatement databaseName, rest)
          _ -> Nothing
      _ -> Nothing
  parseTokens (KeywordToken ShowKeyword
               : KeywordToken DatabasesKeyword
               : SemicolonToken
               : rest) =
    Just (ShowDatabasesStatement, rest)
  parseTokens (KeywordToken CreateKeyword
               : KeywordToken SchemaKeyword
               : rest) =
    case parseTokens rest of
      Just (schemaName, rest) ->
        case rest of
          (SemicolonToken
           : rest) -> Just (CreateSchemaStatement schemaName, rest)
          _ -> Nothing
  parseTokens (KeywordToken DropKeyword
               : KeywordToken SchemaKeyword
               : rest) =
    case parseTokens rest of
      Just (schemaName, rest) ->
        case rest of
          (SemicolonToken
           : rest) -> Just (DropSchemaStatement schemaName, rest)
          _ -> Nothing
      Nothing -> Nothing
  parseTokens _ = Nothing
instance ShowTokens [Statement] where
  showTokens [] = []
  showTokens (statement : rest) = showTokens statement ++ showTokens rest
instance ParseTokens [Statement] where
  parseTokens tokens =
    let visit soFar tokens =
          case parseTokens tokens of
            Nothing -> Just (soFar, tokens)
            Just (statement, rest) -> visit (soFar ++ [statement]) rest
    in visit [] tokens


data TransactionStatus
  = RunningTransactionStatus
  | CommittedTransactionStatus
  | RolledBackTransactionStatus


data Transaction =
  Transaction {
      transactionParent :: Maybe Transaction,
      transactionStatus :: MVar TransactionStatus,
      transactionDatabases :: MVar (Set Database),
      transactionPages :: MVar (Map Int Page)
    }


data PageContent =
  PageContent {
      pageContentArray :: MVar (IOUArray Int Word8)
    }


data Page =
  Page {
      pageIndex :: MVar Int,
      pageContentStack :: MVar (Map Transaction PageContent)
    }


data Database =
  Database {
      databaseID :: Int,
      databaseNames :: MVar (Set Text),
      databaseFilename :: MVar Text,
      databasePages :: MVar (Map Int Page),
      databaseFileHandle :: MVar Handle
    }


data Databases =
  Databases {
      databasesByName :: Map Text Database,
      databasesByFilePath :: Map Text Database
    }


data TeaquelState =
  TeaquelState {
      teaquelStateIDs :: MVar [(Int, Int)],
      teaquelStateDatabases :: MVar Databases,
      teaquelStateTransaction :: MVar (Maybe Transaction)
    }


instance Error Text where
  strMsg string = T.pack string


type Teaquel = ReaderT TeaquelState (ErrorT Text IO)


getTeaquelState :: Teaquel TeaquelState
getTeaquelState = ask


teaquelError :: Text -> Teaquel a
teaquelError headline = throwError headline


allocateID :: Teaquel Int
allocateID = do
  teaquelState <- getTeaquelState
  let theIDsMVar = teaquelStateIDs teaquelState
  theIDs <- takeMVar theIDsMVar
  let loop prospectiveID [] = Just prospectiveID
      loop prospectiveID ((startID, endID) : rest) =
        if startID > prospectiveID
          then Just prospectiveID
          else if endID < maxBound
                 then loop (endID + 1) rest
                 else Nothing
      maybeNewID = loop 0 theIDs
  case maybeNewID of
    Nothing -> do
      putMVar theIDsMVar theIDs
      teaquelError "Out of IDs."
    Just newID -> do
      let loop soFar [] = soFar ++ [(newID, newID)]
          loop soFar (pair@(startID, endID) : rest) =
            if startID > newID
              then if startID == newID + 1
                     then soFar ++ [(newID, endID)] ++ rest
                     else soFar ++ [(newID, newID), pair] ++ rest
              else if endID + 1 == newID
                     then soFar ++ [(startID, newID)] ++ rest
                     else loop (soFar ++ [pair]) rest
          newIDs = loop [] theIDs
      putMVar theIDsMVar newIDs
      return newID


deallocateID :: Int -> Teaquel ()
deallocateID theID = do
  teaquelState <- getTeaquelState
  let theIDsMVar = teaquelStateIDs teaquelState
  theIDs <- takeMVar theIDsMVar
  let newIDs = concat $ map (\pair@(startID, endID) ->
                               if (startID <= theID) && (theID <= endID)
                                 then catMaybes [if startID < theID
                                                   then Just (startID, theID - 1)
                                                   else Nothing,
                                                 if theID < endID
                                                   then Just (theID + 1, endID)
                                                   else Nothing]
                                 else [pair])
                            theIDs
  putMVar theIDsMVar newIDs


withoutTransaction :: Teaquel a -> Teaquel a
withoutTransaction action = do
  state <- getTeaquelState
  let transactionMapMVar = teaquelStateTransaction state
  transactionMap <- readMVar transactionMapMVar
  case transactionMap of
    Nothing -> action
    Just _ -> teaquelError "Cannot be in a transaction."


attachDatabase :: DatabaseName -> FilePath -> Teaquel ()
attachDatabase (DatabaseName name) (FilePath filename) = do
  withoutTransaction $ do
    state <- getTeaquelState
    let databasesMVar = teaquelStateDatabases state
    databases <- takeMVar databasesMVar
    case Map.lookup filename (databasesByFilePath databases) of
      Just database -> do
        names <- takeMVar $ databaseNames database
        let newNames = Set.insert name names
            newDatabases = databases {
                               databasesByName =
                                 Map.insert name database $ databasesByName databases
                             }
        putMVar (databaseNames database) newNames
        putMVar databasesMVar newDatabases
      Nothing -> do
        fileHandle <- liftIO $ openBinaryFile (T.unpack filename) ReadWriteMode
        theID <- allocateID
        namesMVar <- newMVar (Set.singleton name)
        filenameMVar <- newMVar filename
        pagesMVar <- newMVar Map.empty
        fileHandleMVar <- newMVar fileHandle
        let database = Database {
                           databaseID = theID,
                           databaseNames = namesMVar,
                           databaseFilename = filenameMVar,
                           databasePages = pagesMVar,
                           databaseFileHandle = fileHandleMVar
                         }
            newDatabases = databases {
                               databasesByName =
                                 Map.insert name database
                                   $ databasesByName databases,
                               databasesByFilePath=
                                 Map.insert filename database
                                   $ databasesByFilePath databases
                             }
        putMVar databasesMVar newDatabases


main :: IO ()
main = do
  ids <- newMVar []
  databases <- newMVar $ Databases {
                             databasesByName = Map.empty,
                             databasesByFilePath = Map.empty
                           }
  transaction <- newMVar Nothing
  let state = TeaquelState {
                  teaquelStateIDs = ids,
                  teaquelStateDatabases = databases,
                  teaquelStateTransaction = transaction
		}
  _ <- runErrorT $ flip runReaderT state $ do
    let loop [] = return () :: Teaquel ()
        loop (inputLine : restInputLines) = do
          let tokens = fromMaybe [] $ lexTokens inputLine
          case parseTokens tokens of
            Nothing -> liftIO $ putStrLn "No parse."
            Just (statements, rest) -> do
              liftIO $ putStrLn $ T.unpack $ showTextList $ showTokens
                (statements :: [Statement])
              loop restInputLines
    loop ["ATTACH DATABASE main AT 'test.db';",
          "SHOW DATABASES;",
          "DETACH DATABASE main;"]
  return ()
