--- ----------------------------------------------------------------------------
--- This is the main CDBI-module. It provides datatypes and functions to
--- do Database-Queries working with Entities (ER-Model)
---
--- @author Mike Tallarek, extensions by Julia Krone, Michael Hanus
--- ----------------------------------------------------------------------------
module Database.CDBI.ER (
    -- Database Functions
    insertEntry, insertEntries, insertEntryCombined, restoreEntries,
    getEntries, getEntriesCombined, updateEntries, deleteEntries,
    updateEntry, updateEntryCombined, 
    getColumn, getColumnTuple, getColumnTriple, getColumnFourTuple, 
    getColumnFiveTuple, getColumnSixTuple,
    getAllEntries, getCondEntries, getEntryWithKey, getEntriesWithColVal,
    insertNewEntry, deleteEntry, deleteEntryR,
    showDatabaseKey, readDatabaseKey,
    saveDBTerms, restoreDBTerms,
    runQueryOnDB, runTransactionOnDB, runJustTransactionOnDB,
    --CDBI.Connection
    DBAction, Connection, SQLResult, printSQLResults,
    runInTransaction, (>+), (>+=),
    begin, commit, rollback, connectSQLite, disconnect, runWithDB,
    -- Datatypes
    EntityDescription, Value, ColumnDescription,
    Join, SetOp(..), Specifier(..), Table,
    SingleColumnSelect(..), TupleColumnSelect(..),
    TripleColumnSelect(..), FourColumnSelect(..),
    FiveColumnSelect(..), SixColumnSelect(..), TableClause(..),
    CombinedDescription, combineDescriptions, addDescription, 
    innerJoin, crossJoin, caseThen,
    sum, avg, minV, maxV, none, count, 
    singleCol, tupleCol, tripleCol, fourCol, fiveCol, sixCol,
    int, float, char, string, bool, date, col, colNum, colVal,
    -- CDBI.Criteria
    Criteria(..), emptyCriteria, Constraint(Exists, Or, And, Not, None),
    isNull, isNotNull, equal, (.=.), notEqual, (./=.), greaterThan, (.>.),
    lessThan, (.<.), greaterThanEqual, (.>=.), lessThanEqual, (.<=.), like,
    (.~.), between, isIn, (.<->.), Option, ascOrder, descOrder, groupBy,
    groupByCol, having, condition, noHave, Condition(..),
    sumIntCol, sumFloatCol, countCol, avgIntCol, avgFloatCol, minCol, maxCol,
    caseResultInt,
    caseResultFloat, caseResultString, caseResultChar, caseResultBool) where

import Char         ( isDigit )
import FilePath     ( (</>) )
import List         ( intercalate, nub )
import ReadShowTerm ( showQTerm, readQTermListFile, writeQTermListFile )
import Time         ( ClockTime )

import Database.CDBI.Connection
import Database.CDBI.Criteria
import Database.CDBI.Description
import Database.CDBI.QueryTypes

-- -----------------------------------------------------------------------------
-- Database functions with Entities
-- -----------------------------------------------------------------------------

--- Inserts an entry into the database.
--- @param ed - The EntityDescription that describes the entity to be saved
--- @param ent - The entry to be inserted
--- @return a `DBAction` with a void result
insertEntry :: EntityDescription a -> a -> DBAction ()
insertEntry ed ent =
  let query = "insert into '" ++ getTable ed ++
              "' values("++ questionmarks ed ++");"
  in execute query ((getToInsertValues ed) ent)

--- Inserts several entries into the database.
--- @param ed - The EntityDescription that describes the entities to be saved
--- @param xs - The list of entries to be inserted
--- @return A Result without parameter if saving worked or an Error if there
--- was a problem. If one saving operation reports an error, every following
--- saving operation will be aborted but every saving operation up to that
--- point will be executed. If these executed saving operations should also
--- be discarded withTransaction or begin/commit/rollback should be used
insertEntries :: EntityDescription a -> [a] -> DBAction ()
insertEntries ed xs =
  let query = ("insert into '" ++ getTable ed ++
               "' values("++ questionmarks ed ++");")
  in executeMultipleTimes query (map (getToInsertValues ed) xs)

--- Stores entries with their current keys in the database.
--- It is an error if entries with the same key are already in the database.
--- Thus, this operation is useful only to restore a database with saved data.
--- @param en - The EntityDescription that describes the entities to be saved
--- @param xs - The list of entries to stored
--- @return A Result without parameter if saving worked or an Error if there
--- was a problem. If one saving operation reports an error, every following
--- saving operation will be aborted but every saving operation up to that
--- point will be executed. If these executed saving operations should also
--- be discarded withTransaction or begin/commit/rollback should be used
restoreEntries :: EntityDescription a -> [a] -> DBAction ()
restoreEntries en xs =
  let query = ("insert into '" ++ (getTable en) ++
               "' values("++ (questionmarks en) ++");")
  in executeMultipleTimes query (map (getToValues en) xs)

--- Gets entries from the database.
--- @param spec - Specifier All or Distinct
--- @param en - The EntityDescription that describes the entity
--- @param crit - Criteria for the query
--- @param op - oreder-by clause
--- @param limit - int value to limit number of entities returned
--- @return a `DBAction` with a list of entries
getEntries :: Specifier -> 
              EntityDescription a -> 
              Criteria -> 
              [Option] ->
              Maybe Int  -> 
              DBAction [a]
getEntries spec en crit op limit = do
  let query = "select " ++ trSpecifier spec ++"* from '" ++ getTable en ++
                 "' " ++ trCriteria crit ++ trOption op ++ trLimit limit ++";"
  vals <- select query [] (getTypes en)
  return $ map (getToEntity en) vals


--- Gets a single Column from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is 
---                 given, can be empty otherwise
--- @param sels - list of SingleColumnSelects to specify query,
---               if there are more requests than can be combined with 
---               setoperators they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@return a `DBAction`t with a list of a-Values as parameter 
---        (where a is the type of the column)
getColumn :: [SetOp] -> 
             [SingleColumnSelect a] -> 
             [Option] -> 
             Maybe Int ->
             DBAction [a]
getColumn _      []       _       _     = return []
getColumn setops (s:sels) options limit = do
  let query = ((foldl (\quest (so, sel) -> quest ++ trSetOp so ++
                                           trSingleSelectQuery sel )
                      (trSingleSelectQuery s)
                      (zip setops sels))
                ++ trOption options ++ trLimit limit++" ;")
  vals <- select query [] (getSingleType s)
  return (map ((getSingleValFunc s) . head) vals)


--- Gets two Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is
---                 given, can be empty otherwise
--- @param sels - list of TupleColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@return a `DBAction`t with a list of a-Values as parameter 
---        (where a is the type of the column)
getColumnTuple :: [SetOp] -> 
                  [TupleColumnSelect a b] -> 
                  [Option] -> 
                  Maybe Int ->
                  DBAction [(a,b)]
getColumnTuple _      []       _       _     = return []
getColumnTuple setops (s:sels) options limit = do
  let query = ((foldl (\quest (so, sel) -> quest ++ trSetOp so ++
                                           trTupleSelectQuery sel )
                      (trTupleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
      (fun1, fun2) = getTupleValFuncs s
  vals <- select query [] (getTupleTypes s)
  return (map (\ [val1, val2] -> ((fun1 val1),(fun2 val2))) vals)

--- Gets three Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is
---                 given, can be empty otherwise
--- @param sels - list of TripleColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@return a `DBAction`t with a list of a-Values as parameter 
---        (where a is the type of the column)
getColumnTriple :: [SetOp] -> 
                   [TripleColumnSelect a b c] -> 
                   [Option] ->
                   Maybe Int -> 
                   DBAction [(a,b,c)]
getColumnTriple _      []       _       _     = return []
getColumnTriple setops (s:sels) options limit = do
  let query = ((foldl (\quest (so, sel) -> quest ++ trSetOp so ++
                                           trTripleSelectQuery sel )
                      (trTripleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
      (fun1, fun2, fun3) = getTripleValFuncs s
  vals <- select query [] (getTripleTypes s)
  return (map (\ [val1, val2, val3] -> (fun1 val1, fun2 val2, fun3 val3)) vals)

--- Gets four Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is 
---                 given, can be empty otherwise
--- @param sels - list of FourColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@return a `DBAction`t with a list of a-Values as parameter 
---        (where a is the type of the column)
getColumnFourTuple :: [SetOp] 
                   -> [FourColumnSelect a b c d]
                   -> [Option]
                   -> Maybe Int 
                   -> DBAction [(a,b,c,d)]
getColumnFourTuple _      []       _       _     = return []
getColumnFourTuple setops (s:sels) options limit = do
  let query = ((foldl (\quest (so, sel) -> quest ++ trSetOp so ++
                                           trFourTupleSelectQuery sel)
                      (trFourTupleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
      (fun1, fun2, fun3, fun4) = getFourTupleValFuncs s
  vals <- select query [] (getFourTupleTypes s)
  return $ map (\ [val1, val2, val3, val4] -> ((fun1 val1),
                                               (fun2 val2), 
                                               (fun3 val3),
                                               (fun4 val4)))
               vals

--- Gets five Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is
---                 given, can be empty otherwise
--- @param sels - list of FiveColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@return a `DBAction` with a list of a-values
---        (where a is the type of the column)
getColumnFiveTuple :: [SetOp]
                   -> [FiveColumnSelect a b c d e]
                   -> [Option]
                   -> Maybe Int 
                   -> DBAction [(a,b,c,d,e)]
getColumnFiveTuple _      []       _       _     = return []
getColumnFiveTuple setops (s:sels) options limit = do
  let query = ((foldl (\quest (so, sel) -> quest ++ trSetOp so ++
                                           trFiveTupleSelectQuery sel)
                      (trFiveTupleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
      (fun1, fun2, fun3, fun4, fun5) = getFiveTupleValFuncs s
  vals <- select query [] (getFiveTupleTypes s)
  return $ map (\ [val1, val2, val3, val4, val5] -> ((fun1 val1),
                                                     (fun2 val2), 
                                                     (fun3 val3),
                                                     (fun4 val4),
                                                     (fun5 val5)))
               vals
             
--- Gets six Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is
---                 given, can be empty otherwise
--- @param sels - list of SixColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@return a `DBAction` with a list of a-values
---        (where a is the type of the column)
getColumnSixTuple :: [SetOp]
                  -> [SixColumnSelect a b c d e f]
                  -> [Option]
                  -> Maybe Int 
                  -> DBAction [(a,b,c,d,e,f)]
getColumnSixTuple _      []       _       _     = return []
getColumnSixTuple setops (s:sels) options limit = do
  let query = ((foldl (\quest (so, sel) -> quest ++ trSetOp so ++
                                           trSixTupleSelectQuery sel)
                      (trSixTupleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
      (fun1, fun2, fun3, fun4, fun5, fun6) = getSixTupleValFuncs s
  vals <- select query [] (getSixTupleTypes s)
  return $ map (\ [val1, val2, val3, val4, val5, val6] -> ((fun1 val1),
                                                           (fun2 val2), 
                                                           (fun3 val3),
                                                           (fun4 val4),
                                                           (fun5 val5),
                                                           (fun6 val6)))
               vals
             
--- Gets combined entries from the database.
--- @param spec - Specifier Distinct or All
--- @param cd - The CombinedDescription that describes the entity
--- @param joins - joins to combine the entity, they will be applied
---                in a left-associative manner 
--- @param crit - Criteria for the query
--- @param op - order-by-clause
--- @param limit - int value to determine number of values returned
--- @return A `DBAction` with a list of entries
getEntriesCombined :: Specifier -> 
                      CombinedDescription a -> 
                      [Join] ->  
                      Criteria ->
                      [Option] ->
                      Maybe Int -> 
                      DBAction [a]
getEntriesCombined spec cd@(CD _ f _ _) joins crit op limit = do
  let query = "select "++ trSpecifier spec ++ "* from " ++ 
              getJoinString cd joins ++ " " ++
              trCriteria crit ++ trOption op ++ trLimit limit ++ ";"
  xs <- select query [] (getJoinTypes cd)
  return (map f xs)

--- Inserts combined entries.
--- @param cd  - The CombinedDescription that describes the entity
--- @param ent - The combined Entity to be inserted
--- @return A Result without parameter if saving worked or an Error if there
--- was a problem
insertEntryCombined :: CombinedDescription a -> a -> DBAction ()
insertEntryCombined (CD desc _ _ f3) ent = mapM_ save (zip desc (f3 ent))
  where
    save :: ((Table, Int, [SQLType]), [SQLValue]) -> DBAction ()
    save ((table, _, types), vals) =
      let query = "insert into '" ++ table ++
                  "' values(" ++ questionmarksHelp (length types) ++");"
      in execute query vals

--- Updates entries depending on wether they fulfill the criteria or not
--- @param en - The EntityDescription descriping the Entities that are to be
--- updated
--- @param xs - A list of ColVal that descripe the columns that should be
--- updated and which values are to be inserted into them
--- @param const - A Constraint can be be given as input which describes
--- which entities should be updated. Only entities fulfilling the constraint
--- will be updated. Nothing updates every entry.
--- @return A Result without parameter if everything went right,
--- an Error if something went wrong
updateEntries :: EntityDescription a -> [ColVal] -> Constraint -> DBAction ()
updateEntries en (ColVal cl val : ys) const =
  let query = "update '" ++ (getTable en) ++ "' set " ++
              foldl
                 (\a (ColVal c v) -> 
                    a ++ ", " ++ (getColumnSimple c) 
                    ++ " = " ++ (trValue v))
                 (getColumnSimple cl ++ " = " ++ trValue val)
                 ys ++
              " " ++ trCriteria (Criteria const Nothing) ++ ";"           
  in execute query []

--- Updates an entry by ID. Works for Entities that have a primary key
--- as first value.
--- This operation updates the entry in the database with the ID
--- of the entry that is given as parameter with the values of
--- the entry given as parameter.
--- @param ed    - The EntityDescription describung the entity-type
--- @param entry - The entry that will be updated
--- @return A Result without parameter if everything went right,
--- an Error if something went wrong
updateEntry :: EntityDescription a -> a -> DBAction ()
updateEntry ed ent = do
  let table = getTable ed
  columns <- getColumnNames table
  let values     = getToValues ed ent
      SQLInt key = head values
      keycol     = head columns
      colvals    = zipWith (colValAlt table) (tail columns) (tail values)
      column     = Column ("\"" ++ keycol ++ "\"")
                          ("\"" ++ table ++ "\".\"" ++ keycol ++ "\"")
      const      = col column .=. int key
  updateEntries ed colvals const

--- Same as updateEntry but for combined Data
updateEntryCombined :: CombinedDescription a -> a -> DBAction ()
updateEntryCombined (CD desc _ f2 _) ent = mapM_ update (zip desc (f2 ent))
 where
  update :: ((Table, Int, [SQLType]), [SQLValue]) -> DBAction ()
  update ((table, _, _), values) = do
    let SQLInt key = (\(x:_) -> x) values
        const = col (Column "\"Key\"" ("\"" ++ table ++ "\".\"Key\"")) 
                .=. int key
    columns <- getColumnNames table
    let ((col1,val1):colval) = zip columns values
    execute ("update '" ++ table ++ "' set " ++
            (foldl (\a (c, v) -> a ++ ", " ++
                                 c ++ " = " ++ (valueToString v))
                   (col1 ++ " = " ++ (valueToString val1))
                   colval) ++
             " " ++ trCriteria (Criteria const Nothing) ++ ";") []

--- Deletes entries depending on wether they fulfill the criteria or not
--- @param en - The EntityDescription descriping the Entities that are to
--- be updated
--- @param const - A Constraint can be be given as input which describes
--- which entities should be deleted. Only entities fulfilling the constraint
--- will be deleted. Nothing deletes every entry.
--- @return A Result without parameter if everything went right, an
--- Error if something went wrong
deleteEntries :: EntityDescription a -> (Maybe Constraint) -> DBAction ()
deleteEntries en const =
  let query = "delete from '" ++ (getTable en) ++ "' " ++
              (case const of
                 Just c -> "where "++(trConstraint c)
                 _      -> "") ++ ";"
  in execute query []

--- Drops a table from the database
--- @param en - The EntitiyDescription that describes the entity of which the
--- table should be dropped
--- @param conn -  A Connection to a database which will be used for this
--- @return A Result without paramter if everything went right, an Error if
--- something went wrong
dropTable :: EntityDescription a -> DBAction ()
dropTable en =
  let query = "drop table '" ++ getTable en ++ "';"
  in execute query []
  
-- -----------------------------------------------------------------------------
-- Auxiliary Functions
-- -----------------------------------------------------------------------------

-- Create a String consisting of '?' seperated by commas corresponding to the
-- number of Types an Entity has
questionmarks :: EntityDescription a -> String
questionmarks en = questionmarksHelp (length (getTypes en))

questionmarksHelp :: Int -> String
questionmarksHelp n = intercalate ", " (replicate n "'?'")

-- Create the join String of a CombinedDescription
getJoinString :: CombinedDescription a -> [Join] -> String
getJoinString (CD nametype _ _ _) jns= getJoinString' nametype jns
  where getJoinString' ((n,r,_):xs) joins =
          "'" ++ (foldl
                    (\a (table, ren, _ ,join) -> a ++ (trJoinPart1 join) ++" '" ++
                                           table ++ "'" ++ (asTable table ren) ++
                                           (trJoinPart2 join))
                    (n ++ "'" ++ (asTable n r))
                    (mapJns xs joins))
        mapJns [] _ = []
        mapJns ((tb, al, t):xs) (j:js) = ((tb, al , t, j): (mapJns xs js))

-- Get the types of a CombinedDescription
getJoinTypes :: CombinedDescription a -> [SQLType]
getJoinTypes (CD nametype _ _ _) = getJoinString' (unzip3 nametype)
    where getJoinString' ((_, _, t:types)) = foldl (\a b -> a ++ b) t types


-----------------------------------------------------------------------------
--- Gets the key of the last inserted entity from the database.
--- @param en - The EntityDescription that describes the entity
--- @return A Result with a key or an Error if something went wrong.
getLastInsertedKey :: EntityDescription a -> DBAction Int
getLastInsertedKey en = do
  let query = "select distinct last_insert_rowid() from '" ++ getTable en ++
                 "';"
  r <- select query [] [SQLTypeInt]
  selectInt r
 where
  selectInt vals = case vals of
    [[key]] -> maybe (failDB unknownKeyError)
                     return
                     (Database.CDBI.Description.intOrNothing key)
    _ -> failDB unknownKeyError

  unknownKeyError =
    DBError UnknownError "Key of inserted entity not available"

-----------------------------------------------------------------------------
-- Some auxiliary operations for translating ER models into Curry
-- with the erd2curry tool.

--- Gets all entries of an entity stored in the database.
--- @param endescr - the EntityDescription describing the entities
--- @return a DB result with the list of entries if everything went right,
---          or an error if something went wrong
getAllEntries :: EntityDescription a -> DBAction [a]
getAllEntries endescr =
  getEntries All endescr (Criteria None Nothing) [] Nothing

--- Gets all entries of an entity satisfying a given condition.
--- @param endescr - the EntityDescription describing the entities
--- @param cond    - a predicate on entities
--- @return a DB result with the list of entries if everything went right,
---          or an error if something went wrong
getCondEntries :: EntityDescription a -> (a -> Bool) -> DBAction [a]
getCondEntries endescr encond = do
  vals <- getAllEntries endescr
  return (filter encond vals)

--- Gets an entry of an entity with a given key.
--- @param endescr   - the EntityDescription describing the entities
--- @param keycolumn - the column containing the primary key
--- @param keyval    - the id-to-value function for entities
--- @param key       - the key of the entity to be fetched
--- @return a DB result with the entry if everything went right,
---         or an error if something went wrong
getEntryWithKey :: Show kid => EntityDescription a -> Column k
                -> (kid -> Value k) -> kid -> DBAction a
getEntryWithKey endescr keycolumn keyval key = do
  vals <- getEntries All endescr
            (Criteria (equal (colNum keycolumn 0) (keyval key)) Nothing)
            []
            Nothing
  if null vals then failDB keyNotFoundError
               else return (head vals)
 where
  keyNotFoundError =
    DBError UnknownError $
            "'" ++ getTable endescr ++ "' entity with key '" ++
            show key ++ "' not available"

--- Get all entries of an entity where some column have a given value.
--- @param endescr   - the EntityDescription describing the entities
--- @param valcolumn - the column containing the required value
--- @param val       - the value required for fetched entities
--- @return a DB result with the entry if everything went right,
---         or an error if something went wrong
getEntriesWithColVal :: EntityDescription a -> Column k -> Value k
                     -> DBAction [a]
getEntriesWithColVal endescr valcolumn val =
  getEntries All endescr
    (Criteria (equal (colNum valcolumn 0) val) Nothing)
    []
    Nothing

--- Inserts a new entry of an entity and returns the new entry with the new key.
--- @param endescr - the EntityDescription describing the inserted entities
--- @param setkey - the operation to set the key of an entry
--- @param keycons - the constructor for entity keys
--- @param entity - the entity to be inserted
--- @return a DB result without a value if everything went right, or an
---         error if something went wrong
insertNewEntry :: EntityDescription a -> (a -> k -> a) -> (Int -> k) -> a
               -> DBAction a
insertNewEntry endescr setkey keycons entity = do
  insertEntry endescr entity
  key <- getLastInsertedKey endescr
  return $ setkey entity (keycons key)

--- Deletes an existing entry from the database.
--- @param endescr - the EntityDescription describing the entities to be deleted
--- @param keycolumn - the column containing the primary key
--- @param keyval - the mapping from entities to their primary keys as SQL vals
--- @param entity - the entity to be deleted
--- @return a DB result without a value if everything went right, or an
---         error if something went wrong
deleteEntry :: EntityDescription a -> Column k -> (a -> Value k) -> a
            -> DBAction ()
deleteEntry endescr keycolumn keyval entity =
  deleteEntries endescr (Just (equal (colNum keycolumn 0) (keyval entity)))

--- Deletes an existing binary relation entry from the database.
--- @param endescr - the EntityDescription describing the entities to be deleted
--- @param keycol1 - the column containing the first key
--- @param keyval1 - the value of the first key to be deleted
--- @param keycol2 - the column containing the second key
--- @param keyval2 - the value of the second key to be deleted
--- @return a DB result without a value if everything went right, or an
---         error if something went wrong
deleteEntryR :: EntityDescription a -> Column k1 -> Value k1
             -> Column k2 -> Value k2 -> DBAction ()
deleteEntryR endescr keycol1 keyval1 keycol2 keyval2 =
  deleteEntries endescr (Just (And [equal (colNum keycol1 0) keyval1,
                                    equal (colNum keycol2 0) keyval2]))

--- Shows a database key for an entity name as a string.
--- Useful if a textual representation of a database key is necessary,
--- e.g., as URL parameters in web pages. This textual representation
--- should not be used to store database keys in attributes!
showDatabaseKey :: String -> (enkey -> Int) -> enkey -> String
showDatabaseKey enname fromenkey enkey = enname ++ show (fromenkey enkey)

--- Transforms a string into a key for an entity name.
--- Nothing is returned if the string does not represent a reasonable key.
readDatabaseKey :: String -> (Int -> enkey) -> String -> Maybe enkey
readDatabaseKey enname toenkey s =
  let (ens,ks) = splitAt (length enname) s
   in if ens==enname && all isDigit ks
        then Just (toenkey (read ks))
        else Nothing

-- Saves all entries of an entity as terms in a file.
--- @param endescr - the EntityDescription of the entities to be saved
--- @param dbname  - name of the database (e.g. "database.db")
--- @param path    - directory where term file is written
saveDBTerms :: Show a => EntityDescription a -> String -> String -> IO ()
saveDBTerms endescr dbname path = do
  allentries <- runQueryOnDB dbname (getAllEntries endescr)
  let savefile = path </> getTable endescr ++ ".terms"
  if null path
   then putStrLn (unlines (map showQTerm allentries)) -- show only
   else do putStr $ "Saving into '" ++ savefile ++ "'..."
           writeQTermListFile savefile allentries
           putStrLn "done"

--- Restores entries saved in a term file by deleting all existing entries
--- and inserting the saved entries.
--- @param endescr - the EntityDescription of the entities to be restored
--- @param dbname  - name of the database (e.g. "database.db")
--- @param path    - directory where term file was saved
restoreDBTerms :: Read a => EntityDescription a -> String -> String -> IO ()
restoreDBTerms endescr dbname path = do
  let savefile = path </> getTable endescr ++ ".terms"
  putStr $ "Restoring from '" ++ savefile ++ "'..."
  entries <- readQTermListFile savefile
  runJustTransactionOnDB dbname
    (deleteEntries endescr Nothing >+ restoreEntries endescr entries)
  putStrLn "done"

--- Executes a DB action on a database and returns the result.
--- An error is raised if the DB action produces an error.
--- @param dbname   - name of the database (e.g. "database.db")
--- @param dbaction - a database action
--- @return the result of the action
runQueryOnDB :: String -> DBAction a -> IO a
runQueryOnDB dbname dbaction =
  runWithDB dbname dbaction >>= return . fromSQLResult

--- Executes a DB action as a transcation on a database and returns the result.
--- If the DB action produces an error, the transaction is rolled back
--- and the error is returned, otherwise the transaction is committed.
--- @param str - name of the database (e.g. "database.db")
--- @param dbaction - a database action
--- @return the result of the action
runTransactionOnDB :: String -> DBAction a -> IO (SQLResult a)
runTransactionOnDB dbname dbaction =
  runWithDB dbname (runInTransaction dbaction)

--- Executes a DB action as a transcation on a database and returns the result.
--- An error is raised if the DB action produces an error so that the
--- transaction is rolled back.
--- @param str - name of the database (e.g. "database.db")
--- @param dbaction - a database action
--- @return the result of the action
runJustTransactionOnDB :: String -> DBAction a -> IO a
runJustTransactionOnDB dbname dbaction =
  runWithDB dbname (runInTransaction dbaction) >>= return . fromSQLResult

----------------------------------------------------------------------------
