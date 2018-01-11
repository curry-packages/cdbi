--- ----------------------------------------------------------------------------
--- This module defines basis data types and functions for accessing
--- database systems using SQL. Currently, only SQLite3 is supported,
--- but this is easy to extend. It also provides execution of SQL-Queries 
--- with types. Allowed datatypes for these queries are defined and
--- the conversion to standard SQL-Queries is provided.
---
--- @author Mike Tallarek, Michael Hanus
--- ----------------------------------------------------------------------------
module Database.CDBI.Connection
  ( -- Basis types and operations
    SQLValue(..), SQLType(..), SQLResult, fromSQLResult, printSQLResults
  , DBAction, DBError (..), DBErrorKind (..), Connection (..)
    -- DBActions
  , runDBAction, runInTransaction, returnDB, failDB, (>+), (>+=)
  , executeRaw, execute, select
  , executeMultipleTimes, getColumnNames, valueToString
    -- Connections
  , connectSQLite, disconnect, begin, commit, rollback, runWithDB
  ) where

import Char         ( isDigit )
import Function     ( on )
import Global       ( Global, GlobalSpec(..), global, readGlobal, writeGlobal )
import IOExts       ( connectToCommand )
import IO           ( Handle, hPutStrLn, hGetLine, hFlush, hClose, stderr )
import List         ( init, insertBy, isInfixOf, isPrefixOf, tails )
import ReadShowTerm ( readQTerm, readsQTerm, showQTerm )
import ReadNumeric  ( readInt )
import System       ( system )
import Time

import Text.CSV     ( readCSV )

infixl 1 >+, >+=

--- Global flag for database debug mode.
--- If on, all communication with database is written to stderr.
dbDebug :: Bool
dbDebug = False

--- If this flag is true, the SQL output will be requested in csv format
--- (which can be parsed faster than the line mode output).
dbWithCSVMode :: Bool
dbWithCSVMode = True

-- -----------------------------------------------------------------------------
-- Datatypes 
-- -----------------------------------------------------------------------------

--- The result of SQL-related actions. It is either a `DBError` or some value.
type SQLResult a = Either DBError a

--- Gets the value of an 'SQLResult'. If there is no result value
--- but a database error, the error is raised.
fromSQLResult :: SQLResult a -> a
fromSQLResult (Left  err) = error $ "Database connection error: " ++ show err
fromSQLResult (Right val) = val

--- Print an 'SQLResult' list, i.e., print either the 'DBError'
--- or the list of result elements.
printSQLResults :: Show a => SQLResult [a] -> IO ()
printSQLResults (Left  err) = putStrLn $ show err
printSQLResults (Right res) = mapIO_ print res


--- `DBError`s are composed of an `DBErrorKind` and a `String`
--- describing the error more explicitly.
data DBError = DBError DBErrorKind String
 deriving (Eq,Show)

--- The different kinds of errors.
data DBErrorKind
  = TableDoesNotExist
  | ParameterError
  | ConstraintViolation
  | SyntaxError
  | NoLineError
  | LockedDBError
  | UnknownError
 deriving (Eq,Show)

--- Data type for SQL values, used during the communication with the database.
data SQLValue
  = SQLString String
  | SQLInt    Int
  | SQLFloat  Float
  | SQLChar   Char
  | SQLBool   Bool
  | SQLDate   ClockTime
  | SQLNull
  deriving Show

--- Type identifiers for `SQLValue`s, necessary to determine the type
--- of the value a column should be converted to.
data SQLType
  = SQLTypeString
  | SQLTypeInt
  | SQLTypeFloat
  | SQLTypeChar
  | SQLTypeBool
  | SQLTypeDate
  
-- -----------------------------------------------------------------------------
-- Database actions with types
-- -----------------------------------------------------------------------------

--- A `DBAction` takes a connection and performs an IO action that
--- returns a `SQLResult a` value.
data DBAction a = DBAction (Connection -> IO (SQLResult a))

--- Runs a `DBAction` on a connection.
runDBAction :: DBAction a -> Connection -> IO (SQLResult a)
runDBAction (DBAction a) conn = a conn

--- Run a `DBAction` as a transaction.
--- In case of an `Error` it will rollback all changes, otherwise the changes
--- are committed.
--- @param act  - The `DBAction`
--- @param conn - The `Connection` to the database on which the transaction
--- shall be executed.
runInTransaction :: DBAction a -> DBAction a
runInTransaction act = DBAction $ \conn -> do
  begin conn
  res <- runDBAction act conn
  case res of
    Left  _ -> rollback conn >> return res
    Right _ -> commit   conn >> return res

--- Connects two `DBAction`s.
--- When executed this function will execute the first `DBAction`
--- and then execute the second applied to the result of the first action.
--- A database error will stop either action.
--- @param x - The `DBAction` that will be executed first
--- @param y - The `DBAction` hat will be executed afterwards
--- @return A `DBAction` that wille execute both `DBAction`s.
---         The result is the result of the second `DBAction`.
(>+=) :: DBAction a -> (a -> DBAction b) -> DBAction b
m >+= f = DBAction $ \conn -> do
  v1 <- runDBAction m conn
  case v1 of
    Right val -> runDBAction (f val) conn
    Left  err -> return (Left err)

--- Connects two `DBAction`s but ignore the result of the first.
(>+) :: DBAction a -> DBAction b -> DBAction b
(>+) x y = x >+= (\_ -> y)

--- Returns an `SQLResult`.
returnDB :: SQLResult a -> DBAction a
returnDB r = DBAction $ \_ -> return r

--- A failed `DBAction` with a specific error.
failDB :: DBError -> DBAction a
failDB err = returnDB (Left err)

--- The `Monad` instance of `DBAction`.
instance Monad DBAction where
  a1 >>= a2 = a1 >+= a2
  a1 >>  a2 = a1 >+  a2
  return x  = returnDB (Right x)
  fail s    = returnDB (Left (DBError UnknownError s))

-----------------------------------------------------------------------------
--- Execute a query where the result of the execution is returned.
--- @param query - The SQL Query as a String, might have '?' as placeholder
--- @param values - A list of SQLValues that replace the '?' placeholder
--- @param types - A list of SQLTypes that describe the types of the
--- result-tables (e.g. "select * from exampletable" and [SQLTypeInt,
--- SQLTypeFloat, SQLTypeString]
--- when the table exampletable has three columns of type Int, Float and
--- String.) The order of the list has to be the same as the order of the
--- columns in the table
--- @param conn - A Connection to a database where the query will be executed
--- @return A Result with a list of SQLValues which types correspond to
--- the SQLType-List that was given as a parameter if the execution was
--- successful, otherwise an Error
select :: String -> [SQLValue] -> [SQLType] -> DBAction [[SQLValue]]
select query values types =
  executeRaw query (map valueToString values) >+=
  \a -> returnDB (convertValues a types)

--- execute a query without a result
--- @param query - The SQL Query as a String, might have '?' as placeholder
--- @param values - A list of SQLValues that replace the '?' placeholder
--- @param conn - A Connection to a database where the query will be executed
--- @return An empty if the execution was successful, otherwise an error
execute :: String -> [SQLValue] -> DBAction ()
execute query values = 
  executeRaw query (map valueToString values)  >+ return ()
  
--- Executes a query multiple times with different SQLValues without a result
--- @param query - The SQL Query as a String, might have '?' as placeholder
--- @param values - A list of lists of SQLValues that replace the '?'
---                 placeholder (one list for every execution)
--- @return A void result if every execution was successful, otherwise an
---         Error (meaning at least one execution failed). As soon as one
---         execution fails, the rest wont be executed.
executeMultipleTimes :: String -> [[SQLValue]] -> DBAction ()
executeMultipleTimes query values = mapM_ (execute query) values
  
-- -----------------------------------------------------------------------------
-- Database connections
-- -----------------------------------------------------------------------------

--- Data type for database connections.
--- Currently, only connections to a SQLite3 database are supported,
--- but other types of connections could easily be added.
--- List of functions that would need to be implemented:
--- A function to connect to the database, disconnect, writeConnection
--- readRawConnectionLine, parseLines, begin, commit, rollback,
--- and getColumnNames

data Connection = SQLiteConnection Handle

--- Connect to a SQLite Database
--- @param str - name of the database (e.g. "database.db")
--- @return A connection to a SQLite Database
connectSQLite :: String -> IO Connection
connectSQLite db = do
  exsqlite3 <- system "which sqlite3 > /dev/null"
  when (exsqlite3>0) $
    error "Database interface `sqlite3' not found. Please install package `sqlite3'!"
  h <- connectToCommand $ "sqlite3 " ++ db
  hPutAndFlush h (".mode " ++ if dbWithCSVMode then "csv" else "line")
  hPutAndFlush h (".log "  ++ if dbWithCSVMode then "off" else "stdout")
  return $ SQLiteConnection h

--- Disconnect from a database.
disconnect :: Connection -> IO ()
disconnect (SQLiteConnection h) = hClose h

hPutAndFlush :: Handle -> String -> IO ()
hPutAndFlush h s = do
  when dbDebug $ hPutStrLn stderr ("DB>>> " ++ s)
  hPutStrLn h s >> hFlush h

--- Write a `String` to a `Connection`.
writeConnection :: String -> Connection -> IO ()
writeConnection str (SQLiteConnection h) = hPutAndFlush h str

--- Read a line from a `Connection`.
readRawConnectionLine :: Connection -> IO String
readRawConnectionLine (SQLiteConnection h) =
  if dbDebug
   then do inp <- hGetLine h
           hPutStrLn stderr ("DB<<< " ++ inp)
           return inp
   else hGetLine h

--- Begin a transaction.
begin :: Connection -> IO ()
begin conn@(SQLiteConnection _) = writeConnection "begin;" conn

--- Commit a transaction.
commit :: Connection -> IO ()
commit conn@(SQLiteConnection _) = writeConnection "commit;" conn

--- Rollback a transaction.
rollback :: Connection -> IO ()
rollback conn@(SQLiteConnection _) = writeConnection "rollback;" conn

--- Executes an action dependent on a connection on a database
--- by connecting to the datebase. The connection will be kept open
--- and re-used for the next action to this database.
--- @param str - name of the database (e.g. "database.db")
--- @param action - an action parameterized over a database connection
--- @return the result of the action
runWithDB :: String -> DBAction a -> IO (SQLResult a)
runWithDB dbname dbaction =
  ensureSQLiteConnection dbname >>= runDBAction dbaction

--- Executes an action dependent on a connection on a database
--- by connecting and disconnecting to the datebase.
--- @param str - name of the database (e.g. "database.db")
--- @param action - an action parameterized over a database connection
--- @return the result of the action
runWithDB' :: String -> (Connection -> IO a) -> IO a
runWithDB' dbname dbaction = do
  conn <- connectSQLite dbname
  result <- dbaction conn
  disconnect conn
  return result

-- -----------------------------------------------------------------------------
-- Executing SQL statements
-- -----------------------------------------------------------------------------

--- Executes an SQL statement.
--- The statement may contain '?' placeholders and a list of parameters which
--- should be inserted at the respective positions.
--- The result is a list of list of strings where every single list
--- represents a row of the result.
executeRaw :: String -> [String] -> DBAction [[String]]
executeRaw query para =
  case insertParams query para of
    Left err -> failDB err
    Right qu -> DBAction $ \conn -> do
      writeConnection qu conn 
      parseLines conn
  

--- Returns a list with the names of every column in a table
--- The parameter is the name of the table and a connection
getColumnNames :: String -> DBAction [String]
-- SQLite Implementation
getColumnNames table = DBAction $ \conn -> do
  writeConnection ("pragma table_info(" ++ table ++ ");") conn
  result <- parseLines conn
  case result of
    Left err -> return (Left err)
    Right xs -> return (Right (map retrieveColumnNames xs))
 where
  retrieveColumnNames xs = case xs of
    (_:y:_) -> y
    _       -> error "Database.CDBI.Connection.getColumnNames: wrong arguments"

--- Read every output line of a Connection and return a Result with a list
--- of lists of strings where every list of strings represents a row.
--- NULL-Values have to be empty Strings instead of "NULL", all other
--- values should be represented exactly as they are saved in the database
parseLines :: Connection -> IO (SQLResult [[String]])
--- SQLite Implementation
parseLines conn@(SQLiteConnection _) = do
  random <- getRandom
  case random of
    Left  err -> return (Left err)
    Right val -> do
      writeConnection ("select '" ++ val ++ "';") conn
      parseSQLOutputUntil val conn

--- `getRandom` requests a random number from a SQLite-database.
getRandom :: IO (SQLResult String)
getRandom = do
  conn <- ensureSQLiteConnection "" -- connectSQLite ""
  writeConnection "select hex(randomblob(8));" conn
  result <- readConnectionLine conn
  --disconnect conn
  return result

--- Inserts parameters into a SQL query for placeholders denoted by (`?`).
--- The parameters are inserted in the order in which they are supplied.
--- Will throw error if the number of placeholders isn't equal to the length
--- of the list
insertParams :: String -> [String] -> SQLResult String
insertParams qu xs =
  if (length xs == (countPlaceholder qu)) 
    then Right (insertParams' qu xs)
    else Left (DBError ParameterError 
               "Amount of placeholders not equal to length of placeholder-list")

  where 
  insertParams' sql []            = sql
  insertParams' sql params@(p:ps) = case sql of
    ""             -> ""
    ''':'?':''':cs -> p ++ insertParams' cs ps
    c:cs           -> c : insertParams' cs params
  countPlaceholder qu2 = case qu2 of
      ""             -> 0
      ''':'?':''':cs -> 1 + (countPlaceholder cs)
      _:cs           -> countPlaceholder cs 


--- Reads the current output of SQLite line by line until a specific stop
--- string is found. This is necessary because it is otherwise not possible
--- to determine the end of the output without blocking. (SQLite-Function)
parseSQLOutputUntil :: String -> Connection -> IO (SQLResult [[String]])
parseSQLOutputUntil = if dbWithCSVMode then parseCSVUntil else parseLinesUntil

parseCSVUntil :: String -> Connection -> IO (SQLResult [[String]])
parseCSVUntil stop conn = do
  output <- readLinesUntil
  case output of Left err -> return $ Left err
                 Right csvlines -> return $ Right (concatMap readCSV csvlines)
 where
  readLinesUntil = do
    line <- readConnectionLine conn
    case line of
      Left err -> return $ Left err
      Right s -> if s == stop
                   then return $ Right []
                   else do rest <- readLinesUntil
                           case rest of Left err -> return $ Left err
                                        Right ls -> return $ Right (s:ls)
  
parseLinesUntil :: String -> Connection -> IO (SQLResult [[String]])
parseLinesUntil stop conn@(SQLiteConnection _) = next
  where
  next = do
    value <- readConnectionLine conn
    case value of
      Left (DBError NoLineError "") -> do
            rest <- next
            case rest of
              Left err -> return $ Left err
              Right xs -> return $ Right ([]:xs)
      Left err  -> readRawConnectionLine conn >> return (Left err)
      Right val
        | val == "index" -> next
        | val == stop -> return (Right [[]])
        | otherwise -> do
            rest <- next
            case rest of
              Left  err         -> return $ Left err
              Right ([]:xs)     -> return $ Right ([val]:xs)
              Right ((x:ys):xs) -> return $ Right ((val:(x:ys)):xs)
              Right []          ->
               error "Database.CDBI.Connection.parseLinesUntil: wrong arguments"

--- Read a line from a SQLite Connection and check if it represents a value
readConnectionLine :: Connection -> IO (SQLResult String)
readConnectionLine conn =
  check `liftIO` readRawConnectionLine conn
 where
  --- Ensure that a line read from a database connection represents a value.
  check :: String -> SQLResult String
  check s = if dbWithCSVMode then checkCSV s else checkLine s
  
  checkCSV s | "Error" `isPrefixOf` s
             = Left (DBError (getErrorKindSQLite s) s)
             | otherwise
             = Right s
            
  checkLine s | null s
              = Left (DBError NoLineError "")
              | "Error" `isPrefixOf` s
              = Left (DBError (getErrorKindSQLite s) s)
              | '=' `elem` s
              = Right (getValue s)
              | "automatic index on" `isInfixOf` s
              = Right "index"
              | otherwise
              = Left (DBError (getErrorKindSQLite s) s)
            
--- Get the value from a line with a '='
getValue :: String -> String
  --getValue (_ ++ "= " ++ b) = b
  --getValue (_ ++ "=") = ""
  -- alternative implementation to avoid non-deterministic functional patterns:
getValue s =
 if "case" `isInfixOf` s
    then getCaseValue s
    else
      let taileq = tail (snd (break (== '=') s))
       in if null taileq then "" else let (' ':val) = taileq
                                        in val
 where
  getCaseValue str = getValue (readTilEnd str)
  readTilEnd rest = head (filter (\ls -> "end" `isPrefixOf` ls) (tails rest))

--- Identify the error kind.
getErrorKindSQLite :: String -> DBErrorKind
getErrorKindSQLite str
    | "Error: UNIQUE constraint" `isPrefixOf` str = ConstraintViolation
    | "no such table"            `isInfixOf`  str = TableDoesNotExist
    | "syntax error"             `isInfixOf`  str = SyntaxError
    | "Error: database is locked" `isInfixOf` str = LockedDBError
    | otherwise                                   = UnknownError


-- -----------------------------------------------------------------------------
-- Auxiliary Functions
-- -----------------------------------------------------------------------------


-- Converts an SQLValue to its string representation.
valueToString :: SQLValue -> String
valueToString x = replaceEmptyString $
  case x of
    SQLString a            -> "'" ++ encodeStringToSQL a ++ "'"
    SQLChar a              -> "'" ++ encodeStringToSQL [a] ++ "'"  
    SQLNull                -> "NULL" 
    SQLDate a              -> "'" ++ show (toUTCTime a) ++ "'"
    SQLInt a               -> show a--"'" ++ show a ++ "'"
    SQLFloat a             -> show a --"'" ++ show a ++ "'"
    SQLBool a              -> "'" ++ show a ++ "'"

-- Replaces an empty String with "NU'LL"
replaceEmptyString :: String -> String
replaceEmptyString str = case str of
  "''" -> "NULL"
  st   -> st

-- Converts String representations of SQLValues to their SQLValues
-- Every list of strings in the first parameter represents a data-type
-- of multiple values
-- The list of SQLTypes tells the function what kind of SQLValues should be parsed
convertValues :: [[String]] -> [SQLType] -> SQLResult [[SQLValue]]
convertValues [] _ = Right [] -- this rule should not be used
convertValues (s:str) types =
  if length s == length types
    then Right (map (\x -> map convertValue (zip x types)) (s:str))
    else if null s
           then Right []
           else Left (DBError ParameterError
                        "Number of returned parameters and types not equal")

convertValue :: (String,SQLType) -> SQLValue
convertValue (s, SQLTypeString) = if null s
                                    then SQLNull
                                    else SQLString (decodeStringFromSQL s)

convertValue (s, SQLTypeInt) =
  case readInt s of
    Just (a,_)   -> SQLInt a
    Nothing      -> SQLNull

convertValue (s, SQLTypeFloat) =
  if isFloat s
    then case readsQTerm s of
           []         -> SQLNull
           ((a,_):_)  -> SQLFloat a
    else SQLNull

convertValue (s, SQLTypeBool) =
  case readsQTerm s of
    [(True,[])]  -> SQLBool True
    [(False,[])] -> SQLBool False
    _            -> SQLNull

convertValue (s, SQLTypeDate) =
  case readsQTerm s of
    [(CalendarTime a b c d e f g, [])]
       -> SQLDate (toClockTime (CalendarTime a b c d e f g))
    _  -> SQLNull

convertValue ("", SQLTypeChar) = SQLNull

convertValue (s:_, SQLTypeChar) = SQLChar s


-- Encodes a Curry string into an SQL string which allows an appropriate
-- parsing of SQL output values. This is done by:
-- 1. Transform the string by applying Curry's `show` operation and removing
--    the enclosing apostrophs (i.e., encode all special chars).
-- 2. Replacing all apostrophes in the resulting string with double apostrophes
--    (this is necessary to transfer the encoded string correctly to SQLite)
encodeStringToSQL :: String -> String
encodeStringToSQL s = doubleQuote (init (tail (showQTerm s)))
 where
  doubleQuote "" = ""
  doubleQuote (c:cs) | c == '''  = "''" ++ doubleQuote cs
                     | otherwise = c : doubleQuote cs

-- Decodes SQL string back into a Curry string into an SQL string.
decodeStringFromSQL :: String -> String
decodeStringFromSQL s = readQTerm ('"' : s ++ ['"'])

-- Does a string represent a Float?
isFloat :: String -> Bool
isFloat [a] = isDigit a
isFloat (a:b:_) = (isDigit a) || (isDigit b && a == '-')
isFloat [] = False

-----------------------------------------------------------------------------
-- A global value that keeps all open database handles.
openDBConnections :: Global [(String,Connection)]
openDBConnections = global [] Temporary

-- Connect to SQLite database. Either create a new connection
-- (and keep it) or re-use a previous connection.
ensureSQLiteConnection :: String -> IO Connection
ensureSQLiteConnection db = do
  dbConnections <- readGlobal openDBConnections
  maybe (addNewConnection dbConnections) return (lookup db dbConnections)
 where
  addNewConnection dbConnections = do
    dbcon <- connectSQLite db
    writeGlobal openDBConnections $ -- sort against deadlock
       insertBy ((<=) `on` fst) (db,dbcon) dbConnections
    return dbcon

-- Performs an action on all open database connections.
withAllDBConnections :: (Connection -> IO _) -> IO ()
withAllDBConnections f = readGlobal openDBConnections >>= mapIO_ (f . snd)

--- Closes all database connections. Should be called when no more
--- database access will be necessary.
closeDBConnections :: IO ()
closeDBConnections = do
  withAllDBConnections disconnect
  writeGlobal openDBConnections []

-----------------------------------------------------------------------------
