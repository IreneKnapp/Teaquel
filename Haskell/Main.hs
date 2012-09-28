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


class ShowText item where
  showText :: item -> Text
class ShowTextList item where
  showTextList :: [item] -> Text


data Keyword
  = CreateKeyword
  | DropKeyword
  | SchemaKeyword
  deriving (Eq)
instance ShowText Keyword where
  showText CreateKeyword = "CREATE"
  showText DropKeyword = "DROP"
  showText SchemaKeyword = "SCHEMA"


data Token
  = KeywordToken Keyword
  | LeftParenthesisToken
  | RightParenthesisToken
  | CommaToken
  | SemicolonToken
  | NameToken Text
instance Eq Token where
  KeywordToken a == KeywordToken b = a == b
  NameToken a == NameToken b = T.toLower a == T.toLower b
  LeftParenthesisToken == LeftParenthesisToken = True
  RightParenthesisToken == RightParenthesisToken = True
  CommaToken == CommaToken = True
  SemicolonToken == SemicolonToken = True
  _ == _ = False
instance ShowText Token where
  showText (KeywordToken keyword) = showText keyword
  showText LeftParenthesisToken = "("
  showText RightParenthesisToken = ")"
  showText CommaToken = ","
  showText SemicolonToken = ";"
  showText (NameToken text) = text
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
tokenLexicalType LeftParenthesisToken = PunctuationTokenLexicalType
tokenLexicalType RightParenthesisToken = PunctuationTokenLexicalType
tokenLexicalType CommaToken = PunctuationTokenLexicalType
tokenLexicalType SemicolonToken = PunctuationTokenLexicalType


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
                             "CREATE" -> Just CreateKeyword
                             "DROP" -> Just DropKeyword
                             "SCHEMA" -> Just SchemaKeyword
                             _ -> Nothing
        in case maybeKeyword of
             Just keyword -> Just (KeywordToken keyword, rest)
             Nothing -> Just (NameToken word, rest)
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


data Statement
  = CreateSchemaStatement Text
  | DropSchemaStatement Text
instance ShowTokens Statement where
  showTokens (CreateSchemaStatement name) =
    [KeywordToken CreateKeyword,
     KeywordToken SchemaKeyword,
     NameToken name,
     SemicolonToken]
  showTokens (DropSchemaStatement name) =
    [KeywordToken DropKeyword,
     KeywordToken SchemaKeyword,
     NameToken name,
     SemicolonToken]
instance ParseTokens Statement where
  parseTokens [] = Nothing
  parseTokens (KeywordToken CreateKeyword
               : KeywordToken SchemaKeyword
               : NameToken name
               : SemicolonToken
               : rest) =
    Just (CreateSchemaStatement name, rest)
  parseTokens (KeywordToken DropKeyword
               : KeywordToken SchemaKeyword
               : NameToken name
               : SemicolonToken
               : rest) =
    Just (DropSchemaStatement name, rest)
  parseTokens _ = Nothing
instance ShowTokens [Statement] where
  showTokens [] = []
  showTokens (statement : rest) = showTokens statement ++ showTokens rest
instance ParseTokens [Statement] where
  parseTokens [] = Nothing
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
      databaseFilename :: MVar Text,
      databasePages :: MVar (Map Int Page)
    }


data TeaquelState =
  TeaquelState {
      teaquelStateIDs :: MVar [(Int, Int)],
      teaquelStateDatabases :: MVar (Map Text Database),
      teaquelStateTransaction :: MVar (Maybe Transaction)
    }


instance Error Text where
  strMsg string = T.pack string


type Teaquel = ErrorT Text (ReaderT TeaquelState IO)


getTeaquelState :: Teaquel TeaquelState
getTeaquelState = lift ask


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
  -- TODO
  return ()



withoutTransaction :: Teaquel a -> Teaquel a
withoutTransaction action = do
  state <- getTeaquelState
  let transactionMapMVar = teaquelStateTransaction state
  transactionMap <- readMVar transactionMapMVar
  case transactionMap of
    Nothing -> action
    Just _ -> teaquelError "Cannot be in a transaction."


attachDatabase :: Text -> Teaquel ()
attachDatabase filename = do
  state <- getTeaquelState
  let databaseMapMVar = teaquelStateDatabases state
  databaseMap <- takeMVar databaseMapMVar
  case Map.lookup filename databaseMap of
    Just _ -> do
      putMVar databaseMapMVar databaseMap
      teaquelError "File already attached."
    Nothing -> do
      theID <- allocateID
      filenameMVar <- newMVar filename
      pagesMVar <- newMVar Map.empty
      let database = Database {
                         databaseID = theID,
                         databaseFilename = filenameMVar,
                         databasePages = pagesMVar
                       }
          newDatabaseMap = Map.insert filename database databaseMap
      putMVar databaseMapMVar newDatabaseMap


main :: IO ()
main = do
  let tokens = showTokens $ CreateSchemaStatement "some_schema"
  putStrLn $ T.unpack $ showTextList tokens
  let tokens = fromMaybe [] $ lexTokens $ "DROP SCHEMA some_schema;"
  case parseTokens tokens of
    Nothing -> putStrLn "No parse."
    Just (statements, rest) -> do
      putStrLn $ T.unpack $ showTextList $ showTokens
        (statements :: [Statement])
      putStrLn $ intercalate ", " $ map (show . T.unpack . showText) rest

