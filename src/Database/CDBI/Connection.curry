--- ----------------------------------------------------------------------------
--- This module defines basis data types and functions for accessing
--- database systems using SQL. Currently, only SQLite3 is supported,
--- but this is easy to extend. It also provides execution of SQL-Queries 
--- with types. Allowed datatypes for these queries are defined and
--- the conversion to standard SQL-Queries is provided.
---
--- @author Mike Tallarek
--- @version 0.2
--- @category database
--- ----------------------------------------------------------------------------
module Database.CDBI.Connection
  ( -- Basis types and operations
    SQLValue(..), SQLType(..), SQLResult, fromSQLResult, printSQLResults
  , DBAction, DBError (..), DBErrorKind (..), Connection (..)
    -- DBActions
  , runInTransaction, fail, ok, (>+), (>+=), executeRaw, execute, select
  , executeMultipleTimes, getColumnNames, valueToString
    -- Connections
  , connectSQLite, disconnect, begin, commit, rollback, runWithDB
  ) where

import Char         ( isDigit )
import Function     ( on )
import Global       ( Global, GlobalSpec(..), global, readGlobal, writeGlobal )
import IOExts       ( connectToCommand )
import IO           ( Handle, hPutStrLn, hGetLine, hFlush, hClose, stderr )
import List         ( insertBy, isInfixOf, isPrefixOf, tails )
import ReadShowTerm ( readsQTerm )
import ReadNumeric  ( readInt )
import System       ( system )
import Time

--- Global flag for database debug mode.
--- If on, all communication with database is written to stderr.
dbDebug :: Bool
dbDebug = False

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
printSQLResults :: SQLResult [_] -> IO ()
printSQLResults (Left  err) = putStrLn $ show err
printSQLResults (Right res) = mapIO_ print res


--- `DBError`s are composed of an `DBErrorKind` and a `String`
--- describing the error more explicitly.
data DBError = DBError DBErrorKind String

--- The different kinds of errors.
data DBErrorKind
  = TableDoesNotExist
  | ParameterError
  | ConstraintViolation
  | SyntaxError
  | NoLineError
  | LockedDBError
  | UnknownError

--- Data type for SQL values, used during the communication with the database.
data SQLValue
  = SQLString String
  | SQLInt    Int
  | SQLFloat  Float
  | SQLChar   Char
  | SQLBool   Bool
  | SQLDate   ClockTime
  | SQLNull
  

--- Type identifiers for `SQLValue`s, necessary to determine the type
--- of the value a column should be converted to.
data SQLType
  = SQLTypeString
  | SQLTypeInt
  | SQLTypeFloat
  | SQLTypeChar
  | SQLTypeBool
  | SQLTypeDate
  
--- A DBAction takes a connection and returns an `IO (SQLResult a)`.
type DBAction a = Connection -> IO (SQLResult a)

-- -----------------------------------------------------------------------------
-- Database actions with types
-- -----------------------------------------------------------------------------

--- Run a `DBAction` as a transaction.
--- In case of an `Error` it will rollback all changes, otherwise the changes
--- are committed.
--- @param act  - The `DBAction`
--- @param conn - The `Connection` to the database on which the transaction
--- shall be executed.
runInTransaction :: DBAction a -> DBAction a
runInTransaction act conn = do
  begin conn
  res <- act conn
  case res of
    Left  _ -> rollback conn >> return res
    Right _ -> commit   conn >> return res

--- Connect two `DBAction`s.
--- When executed this function will execute the first `DBAction`
--- and then execute the second applied to the first result
--- An `Error` will stop either action.
--- @param x - The `DBAction` that will be executed first
--- @param y - The `DBAction` hat will be executed afterwards
--- @return A `DBAction` that wille execute both `DBAction`s.
--- The result is the result of the second `DBAction`.
(>+=) :: DBAction a -> (a -> DBAction b) -> DBAction b
m >+= f = \conn -> do
  v1 <- m conn
  case v1 of
    Right val -> f val conn
    Left  err -> return (Left err)

--- Connect two DBActions, but ignore the result of the first.
(>+) :: DBAction a -> DBAction b -> DBAction b
(>+) x y = x >+= (\_ -> y)

      
--- Failing action.
fail :: DBError -> DBAction a
fail err _ = return (Left err)

--- Successful action.
ok :: a -> DBAction a
ok val _ = return (Right val)

--- execute a query where the result of the execution is returned
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
select query values types conn = 
  (executeRaw query (valuesToString values) >+=
  (\a _ -> return (convertValues a types))) conn

--- execute a query without a result
--- @param query - The SQL Query as a String, might have '?' as placeholder
--- @param values - A list of SQLValues that replace the '?' placeholder
--- @param conn - A Connection to a database where the query will be executed
--- @return An empty if the execution was successful, otherwise an error
execute :: String -> [SQLValue] -> DBAction ()
execute query values conn = 
  (executeRaw query (valuesToString values)  >+
  (\_ -> return (Right ()))) conn
  
--- execute a query multiple times with different SQLValues without a result
--- @param query - The SQL Query as a String, might have '?' as placeholder
--- @param values - A list of lists of SQLValues that replace the '?' placeholder
--- (One list for every execution)
--- @param conn - A Connection to a database where the query will be executed
--- @return A empty Result if every execution was successful, otherwise an
--- Error (meaning at least one execution failed). As soon as one
--- execution fails the rest wont be executed.
executeMultipleTimes :: String -> [[SQLValue]] -> DBAction ()
executeMultipleTimes query values conn = do
  result <- foldl (\res row -> (res >>= \x -> case x of
                                               Right _ -> execute query row conn
                                               error   -> return error))
                  (return (Right ()))
                  values
  return result


-- -----------------------------------------------------------------------------
-- Database connections
-- -----------------------------------------------------------------------------

--- Data type for database connections.
--- Currently, only connections to a SQLite3 database are supported,
--- but other types of connections could easily be added.
--- List of functions that would need to be implemented:
--- A function to connect to the database, disconnect, writeConnection
--- readRawConnection, parseLines, begin, commit, rollback and getColumnNames
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
  hPutAndFlush h ".mode line"
  hPutAndFlush h ".log stdout"
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

--- Read a `String` from a `Connection`.
readRawConnection :: Connection -> IO String
readRawConnection (SQLiteConnection h) =
  if dbDebug
   then do inp <- hGetLine h
           hPutStrLn stderr ("DB<<< " ++ inp)
           return inp
   else hGetLine h

--- Begin a Transaction.
begin :: Connection -> IO ()
begin conn@(SQLiteConnection _) = writeConnection "begin;" conn

--- Commit a Transaction.
commit :: Connection -> IO ()
commit conn@(SQLiteConnection _) = writeConnection "commit;" conn

--- Rollback a Transaction.
rollback :: Connection -> IO ()
rollback conn@(SQLiteConnection _) = writeConnection "rollback;" conn

--- Executes an action dependent on a connection on a database
--- by connecting to the datebase. The connection will be kept open
--- and re-used for the next action to this database.
--- @param str - name of the database (e.g. "database.db")
--- @param action - an action parameterized over a database connection
--- @return the result of the action
runWithDB :: String -> (Connection -> IO a) -> IO a
runWithDB dbname dbaction = ensureSQLiteConnection dbname >>= dbaction

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

--- Execute a SQL statement.
--- The statement may contain '?' placeholders and a list of parameters which
--- should be inserted at the respective positions.
--- The result is a list of list of strings where every single list
--- represents a row of the result.
executeRaw :: String -> [String] -> DBAction [[String]]
executeRaw query para conn = do
  let queryInserted = (insertParams query para)
  case queryInserted of
    Left err -> return $ Left err
    Right qu -> do
      writeConnection qu conn 
      parseLines conn
  

--- Returns a list with the names of every column in a table
--- The parameter is the name of the table and a connection
getColumnNames :: String -> DBAction [String]
-- SQLite Implementation
getColumnNames table conn@(SQLiteConnection _) = do
  writeConnection ("pragma table_info(" ++ table ++ ");") conn
  result <- parseLines conn
  case result of
    Left err -> return (Left err)
    Right xs -> return (Right (map retrieveColumnNames xs))
      where retrieveColumnNames :: [String] -> String
            retrieveColumnNames (_:y:_) = y

--- Read every output line of a Connection and return a Result with a list
--- of lists of Strings where every list of Strings represents a row.
--- NULL-Values have to be empty Strings instead of "NULL", all other
--- values should be represented exactly as they're saved in the database
parseLines :: DBAction [[String]]
--- SQLite Implementation
parseLines conn@(SQLiteConnection _) = do
  random <- getRandom
  case random of
    Left  err -> fail err conn
    Right val -> do
      writeConnection ("select '" ++ val ++ "';") conn
      parseLinesUntil val conn

--- `getRandom` requests a random number from a SQLite-database.
getRandom :: IO (SQLResult String)
getRandom = do
  conn <- ensureSQLiteConnection "" -- connectSQLite ""
  writeConnection "select hex(randomblob(8));" conn
  result <- readConnection conn
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


--- Reads the current output of sqlite line for line until a specific
--- `String` is met. This is necessary because it is otherwise not possible
--- to determine the end of the output without blocking. (SQLite-Function)
parseLinesUntil :: String -> DBAction [[String]]
parseLinesUntil stop conn@(SQLiteConnection _) = next
  where
  next = do
    value <- readConnection conn
    case value of
      Left (DBError NoLineError "") -> do
            rest <- next
            case rest of
              Left err -> fail err conn
              Right xs -> ok ([]:xs) conn
      Left err  -> readRawConnection conn >> fail err conn
      Right val
        | val == "index" -> next
        | val == stop -> ok [[]] conn
        | otherwise -> do
            rest <- next
            case rest of
              Left  err         -> fail err conn
              Right ([]:xs)     -> ok ([val]:xs) conn
              Right ((x:ys):xs) -> ok ((val:(x:ys)):xs) conn

--- Read a line from a SQLite Connection and check if it represents a value
readConnection :: DBAction String
readConnection conn@(SQLiteConnection _) = do check `liftIO` readRawConnection conn
  where
  --- Ensure that a line read from a database connection represents a value.
  check :: String -> SQLResult String
  check str | null str                             = Left (DBError NoLineError "")
            | "Error" `isPrefixOf` str             = Left (DBError (getErrorKindSQLite str) str)
            | '=' `elem` str                       = Right (getValue str)
            | "automatic index on" `isInfixOf` str = Right "index"
            | otherwise
            = Left (DBError (getErrorKindSQLite str) str)
            
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
  where getCaseValue str = getValue (readTilEnd str)
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


-- Converts SQLValues to their String representation
valuesToString :: [SQLValue] -> [String]
valuesToString xs = map valueToString xs

valueToString :: SQLValue -> String
valueToString x = replaceEmptyString $
  case x of
    SQLString a            -> "'" ++ doubleApostrophes a ++ "'"
    SQLChar a              -> "'" ++doubleApostrophes [a] ++ "'"  
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

-- Converts Stringrepresentations of SQLValues to their SQLValues
-- Every list of Strings in the first parameter represents a data-type
-- of multiple values
-- The list of SQLTypes tells the function what kind of SQLValues should be parsed
convertValues :: [[String]] -> [SQLType] -> SQLResult [[SQLValue]]
convertValues (s:str) types =
  if((length s) == (length types))
    then Right (map (\x -> map convertValuesHelp (zip x types)) (s:str))
    else if  ((length s) == 0)
           then Right []
           else Left (DBError ParameterError "Number of returned Parameters and Types not equal")

convertValuesHelp :: (String,SQLType) -> SQLValue
convertValuesHelp ([], SQLTypeString) = SQLNull

convertValuesHelp ((s:str), SQLTypeString) = (SQLString (s:str))

convertValuesHelp (s, SQLTypeInt) =
  case (readInt s) of
    Just (a,_)   -> (SQLInt a)
    Nothing      -> SQLNull

convertValuesHelp (s, SQLTypeFloat) =
  if isFloat s
    then case (readsQTerm s) of
           []         -> SQLNull
           ((a,_):_)  -> SQLFloat a
    else SQLNull

convertValuesHelp (s, SQLTypeBool) =
  case (readsQTerm s) of
    [(True,[])]  -> SQLBool True
    [(False,[])] -> SQLBool False
    _            -> SQLNull

convertValuesHelp (s, SQLTypeDate) =
  case (readsQTerm s) of
    [((CalendarTime a b c d e f g),[])] -> SQLDate (toClockTime (CalendarTime a b c d e f g))
    _                                   -> SQLNull

convertValuesHelp ("", SQLTypeChar) = SQLNull

convertValuesHelp (s:_, SQLTypeChar) = SQLChar s

-- replace all Apostrophes in a string with a doubled apostrophe
doubleApostrophes :: String -> String
doubleApostrophes (s:str) =
  if s == '\''
    then ("''" ++ (doubleApostrophes str))
    else (s : (doubleApostrophes str))

doubleApostrophes "" = ""

-- Does the String represent a Float?
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
