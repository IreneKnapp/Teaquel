{-# LANGUAGE OverloadedStrings, FlexibleInstances #-}
module Main (main) where

import Data.Char
import Data.List
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T


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

