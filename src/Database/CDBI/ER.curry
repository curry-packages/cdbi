--- ----------------------------------------------------------------------------
--- This is the main CDBI-module. It provides datatypes and functions to
--- do Database-Queries working with Entities (ER-Model)
---
--- @author Mike Tallarek, extensions by Julia Krone, Michael Hanus
--- ----------------------------------------------------------------------------
module Database.CDBI.ER (
    -- Database Functions
    saveEntry, saveMultipleEntries, saveEntryCombined,
    insertEntry, insertEntries, restoreEntries,
    getEntries, getEntriesCombined, updateEntries, deleteEntries,
    insertEntryCombined, updateEntry, updateEntryCombined, 
    getColumn, getColumnTuple, getColumnTriple, getColumnFourTuple, 
    getColumnFiveTuple,
    getAllEntries, getCondEntries, getEntryWithKey, getEntriesWithColVal,
    insertNewEntry, deleteEntry, deleteEntryR,
    showDatabaseKey, readDatabaseKey,
    saveDBTerms,
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
    FiveColumnSelect(..), TableClause(..),
    CombinedDescription, combineDescriptions, addDescription, 
    innerJoin, crossJoin, caseThen,
    sum, avg, minV, maxV, none, count, 
    singleCol, tupleCol, tripleCol, fourCol, fiveCol,   
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
import ReadShowTerm ( showQTerm, writeQTermListFile )
import Time         ( ClockTime )

import Database.CDBI.Connection
import Database.CDBI.Criteria
import Database.CDBI.Description
import Database.CDBI.QueryTypes

-- -----------------------------------------------------------------------------
-- Database functions with Entities
-- -----------------------------------------------------------------------------

--- Inserts an entry into the database.
--- @param a - The entry to save
--- @param en - The EntityDescription that describes the entity to be saved
--- @param conn - A Connection to a database which will be used for this
--- @return A Result without parameter if saving worked or an Error if there
--- was a problem
insertEntry :: a -> EntityDescription a -> DBAction ()
insertEntry a en conn = do
  let query = ("insert into '" ++ (getTable en)
              ++ "' values("++ (questionmarks en) ++");")
  execute query ((getToInsertValues en) a) conn

--- Saves an entry to the database (only for backward compatibility).
saveEntry :: a -> EntityDescription a -> DBAction ()
saveEntry = insertEntry

--- Inserts several entries into the database.
--- @param xs - The list of entries to save
--- @param en - The EntityDescription that describes the entities to be saved
--- @param conn - A Connection to a database which will be used for this
--- @return A Result without parameter if saving worked or an Error if there
--- was a problem. If one saving operation reports an error, every following
--- saving operation will be aborted but every saving operation up to that
--- point will be executed. If these executed saving operations should also
--- be discarded withTransaction or begin/commit/rollback should be used
insertEntries :: [a] -> EntityDescription a -> DBAction ()
insertEntries xs en conn = do
  let query = ("insert into '" ++ (getTable en) ++
               "' values("++ (questionmarks en) ++");")
  executeMultipleTimes query (map (getToInsertValues en) xs) conn

--- Saves multiple entries to the database (only for backward compatibility).
saveMultipleEntries :: [a] -> EntityDescription a -> DBAction ()
saveMultipleEntries = insertEntries

--- Stores entries with their current keys in the database.
--- It is an error if entries with the same key are already in the database.
--- Thus, this operation is useful only to restore a database with saved data.
--- @param xs - The list of entries to save
--- @param en - The EntityDescription that describes the entities to be saved
--- @param conn - A Connection to a database which will be used for this
--- @return A Result without parameter if saving worked or an Error if there
--- was a problem. If one saving operation reports an error, every following
--- saving operation will be aborted but every saving operation up to that
--- point will be executed. If these executed saving operations should also
--- be discarded withTransaction or begin/commit/rollback should be used
restoreEntries :: [a] -> EntityDescription a -> DBAction ()
restoreEntries xs en conn = do
  let query = ("insert into '" ++ (getTable en) ++
               "' values("++ (questionmarks en) ++");")
  executeMultipleTimes query (map (getToValues en) xs) conn

--- Gets entries from the database.
--- @param spec - Specifier All or Distinct
--- @param en - The EntityDescription that describes the entity
--- @param crit - Criteria for the query
--- @param op - oreder-by clause
--- @param limit - int value to limit number of entities returned
--- @param conn - A Connection to a database which will be used for this
--- @return A Result with a list of entries as parameter or an Error
--- if something went wrong.
getEntries :: Specifier -> 
              EntityDescription a -> 
              Criteria -> 
              [Option] ->
              Maybe Int  -> 
              DBAction [a]
getEntries spec en crit op limit conn = do
  let query = "select " ++ trSpecifier spec ++"* from '" ++ getTable en ++
                 "' " ++ trCriteria crit ++ trOption op ++ trLimit limit ++";"
  ((select query [] (getTypes en)) >+= 
    (\vals _ -> return $ Right (map (getToEntity en) vals))) 
    conn


--- Gets a single Column from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is 
---                 given, can be empty otherwise
--- @param sels - list of SingleColumnSelects to specify query,
---               if there are more requests than can be combined with 
---               setoperators they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@param conn - A Connection to a database which will be used for this
---@return A Result with a list of a-Values as parameter 
---       (where a is the type of the column) or an Error
---       if something went wrong.
getColumn :: [SetOp] -> 
             [SingleColumnSelect a] -> 
             [Option] -> 
             Maybe Int ->
             DBAction [a]
getColumn _      []       _       _     _   = return $ Right []
getColumn setops (s:sels) options limit conn = do
  let query = ((foldl (\quest (so, sel) -> quest ++ (trSetOp so) ++
                                               (trSingleSelectQuery sel) )
                      (trSingleSelectQuery s)
                      (zip setops sels))
                ++ trOption options ++ trLimit limit++" ;")
  ((select query [] (getSingleType s)) >+= 
      (\vals _ -> return $ Right (map ((getSingleValFunc s). head) vals))) 
      conn 


--- Gets two Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is
---                 given, can be empty otherwise
--- @param sels - list of TupleColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@param conn - A Connection to a database which will be used for this
---@return A Result with a list of a-Values as parameter 
---       (where a is the type of the column) or an Error
---       if something went wrong.     
getColumnTuple :: [SetOp] -> 
                  [TupleColumnSelect a b] -> 
                  [Option] -> 
                  Maybe Int ->
                  DBAction [(a,b)]
getColumnTuple _      []       _       _     _    = return $ Right []
getColumnTuple setops (s:sels) options limit conn = do
  let query = ((foldl (\quest (so, sel) -> quest ++ (trSetOp so) ++
                                               (trTupleSelectQuery sel) )
                      (trTupleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
  let (fun1, fun2) = getTupleValFuncs s
  ((select query [] (getTupleTypes s)) >+= 
      (\vals _ -> return $ Right (map (\[val1, val2] -> ((fun1 val1),(fun2 val2)))
                                      vals))) 
      conn

--- Gets three Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is
---                 given, can be empty otherwise
--- @param sels - list of TripleColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@param conn - A Connection to a database which will be used for this
---@return A Result with a list of a-Values as parameter 
---       (where a is the type of the column) or an Error
---       if something went wrong.           
getColumnTriple :: [SetOp] -> 
                   [TripleColumnSelect a b c] -> 
                   [Option] ->
                   Maybe Int -> 
                   DBAction [(a,b,c)]
getColumnTriple _      []       _       _     _    = return $ Right []                                            
getColumnTriple setops (s:sels) options limit conn = do
  let query = ((foldl (\quest (so, sel) -> quest ++ (trSetOp so) ++
                                               (trTripleSelectQuery sel) )
                      (trTripleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
  let (fun1, fun2, fun3) = getTripleValFuncs s
  ((select query [] (getTripleTypes s)) >+= 
      (\vals _ -> return $ 
                   Right 
                     (map (\[val1, val2, val3] -> ((fun1 val1),
                                                    (fun2 val2), 
                                                     (fun3 val3)))
                          vals))) 
      conn

--- Gets four Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is 
---                 given, can be empty otherwise
--- @param sels - list of FourColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@param conn - A Connection to a database which will be used for this
---@return A Result with a list of a-Values as parameter 
---       (where a is the type of the column) or an Error
---       if something went wrong.   
getColumnFourTuple :: [SetOp] 
                   -> [FourColumnSelect a b c d]
                   -> [Option]
                   -> Maybe Int 
                   -> DBAction [(a,b,c,d)]
getColumnFourTuple _      []       _       _     _   = return $ Right []
getColumnFourTuple setops (s:sels) options limit conn = do
  let query = ((foldl (\quest (so, sel) -> quest ++ (trSetOp so) ++
                                               (trFourTupleSelectQuery sel))
                      (trFourTupleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
  let (fun1, fun2, fun3, fun4) = getFourTupleValFuncs s
  ((select query [] (getFourTupleTypes s)) >+= 
      (\vals _ -> return $ 
                   Right 
                     (map (\[val1, val2, val3, val4] -> ((fun1 val1),
                                                         (fun2 val2), 
                                                         (fun3 val3),
                                                         (fun4 val4)))
                          vals))) 
      conn

--- Gets five Columns from the database. 
--- @param setops - list of Setoperators to combine queries if more than one is
---                 given, can be empty otherwise
--- @param sels - list of FiveColumnSelects to specify queries, if there
---               are more requests than can be combined with setoperators
---               they will be ignored
---@param options - order-by-clause for whole query
---@param limit - value to reduce number of returned rows
---@param conn - A Connection to a database which will be used for this
---@return A Result with a list of a-Values as parameter 
---       (where a is the type of the column) or an Error
---       if something went wrong.     
getColumnFiveTuple :: [SetOp]
                   -> [FiveColumnSelect a b c d e]
                   -> [Option]
                   -> Maybe Int 
                   -> DBAction [(a,b,c,d,e)]
getColumnFiveTuple _      []       _       _     _    = return $ Right []
getColumnFiveTuple setops (s:sels) options limit conn = do
  let query = ((foldl (\quest (so, sel) -> quest ++ (trSetOp so) ++
                                               (trFiveTupleSelectQuery sel))
                      (trFiveTupleSelectQuery s)
                      (zip setops sels))
               ++ trOption options ++ trLimit limit++" ;")
  let (fun1, fun2, fun3, fun4, fun5) = getFiveTupleValFuncs s
  ((select query [] (getFiveTupleTypes s)) >+= 
      (\vals _ -> return $ 
                   Right 
                     (map (\[val1, val2, val3, val4, val5] -> ((fun1 val1),
                                                               (fun2 val2), 
                                                               (fun3 val3),
                                                               (fun4 val4),
                                                               (fun5 val5)))
                      vals))) 
      conn

             
--- Gets combined entries from the database.
--- @param spec - Specifier Distinct or All
--- @param cd - The CombinedDescription that describes the entity
--- @param joins - joins to combine the entity, they will be applied
---                in a left-associative manner 
--- @param crit - Criteria for the query
--- @param op - order-by-clause
--- @param limit - int value to determine number of values returned
--- @param conn - A Connection to a database which will be used for this
--- @return A Result with a list of entries as parameter or an Error
--- if something went wrong.
getEntriesCombined :: Specifier -> 
                      CombinedDescription a -> 
                      [Join] ->  
                      Criteria ->
                      [Option] ->
                      Maybe Int -> 
                      DBAction [a]
getEntriesCombined spec cd@(CD _ f _ _) joins crit op limit conn = do
  let query = "select "++ trSpecifier spec ++ "* from " ++ 
                 (getJoinString cd joins) ++ " " ++
                  (trCriteria crit) ++ trOption op ++ trLimit limit ++ ";"
  result <- select query [] (getJoinTypes cd) conn
  case result of
    Right xs -> return $ Right $ map f xs
    Left err -> return $ Left err
                       
--- Inserts combined entries.
--- @param a - The combined Entity to be saved
--- @param cd - The CombinedDescription that describes the entity
--- @param conn - A Connection to a database which will be used for this
--- @return A Result without parameter if saving worked or an Error if there
--- was a problem
insertEntryCombined :: a -> CombinedDescription a -> DBAction ()
insertEntryCombined ent (CD desc _ _ f3) conn = foldIO save 
                                                     (Right _) 
                                                     (zip desc (f3 ent))
  where
    save :: SQLResult () -> ((Table, Int, [SQLType]), [SQLValue])
         -> IO (SQLResult ())
    save res ((table, _, types), vals) = do
      case res of
        Left err -> return (Left err)
        Right _  -> do let query = ("insert into '" ++ table
                                 ++ "' values(" ++
                                 (questionmarksHelp (length types)) ++");")
                       execute query vals conn

--- Saves combined entries (for backward compatibility).
saveEntryCombined :: a -> CombinedDescription a -> DBAction ()
saveEntryCombined = insertEntryCombined

--- Updates entries depending on wether they fulfill the criteria or not
--- @param en - The EntityDescription descriping the Entities that are to be
--- updated
--- @param xs - A list of ColVal that descripe the columns that should be
--- updated and which values are to be inserted into them
--- @param const - A Constraint can be be given as input which describes
--- which entities should be updated. Only entities fulfilling the constraint
--- will be updated. Nothing updates every entry.
--- @param conn - A Connection to a database which will be used for this
--- @return A Result without parameter if everything went right,
--- an Error if something went wrong
updateEntries :: EntityDescription a -> [ColVal] -> Constraint -> DBAction ()
updateEntries en xs@((ColVal cl val):ys) const conn = do
  let query = "update '" ++ (getTable en) ++ "' set " ++
              (foldl
                 (\a (ColVal c v) -> 
                    a ++ ", " ++ (getColumnSimple c) 
                    ++ " = " ++ (trValue v))
                 ((getColumnSimple cl) ++ " = " ++ (trValue val)) ys) ++
              " " ++ (trCriteria (Criteria const Nothing)) ++ ";"           
  execute query [] conn

--- Updates an entry by ID. Works for Entities that have a primary key
--- as first value.
--- This operation updates the entry in the database with the ID
--- of the entry that is given as parameter with the values of
--- the entry given as parameter.
--- @param entry - The entry that will be updated
--- @param ed -> The EntityDescription describung the entity-type
--- @param conn -> A Connection to a database which will be used for this
--- @return A Result without parameter if everything went right,
--- an Error if something went wrong
updateEntry :: a -> EntityDescription a -> DBAction ()
updateEntry ent ed conn = do
  let table = (getTable ed)
  result <- getColumnNames table conn
  case result of
    Left err -> return (Left err)
    Right columns -> do
      let values = getToValues ed ent
          (SQLInt key) = head values
          keycol = head columns
          colvals = zipWith (colValAlt table) (tail columns) (tail values)
          column = Column ("\"" ++ keycol ++ "\"")
                           ("\"" ++ table ++ "\".\"" ++ keycol ++ "\"")
          const = col column .=. int key
      updateEntries ed colvals const conn   

--- Same as updateEntry but for combined Data
updateEntryCombined :: a -> CombinedDescription a -> DBAction ()
updateEntryCombined ent (CD desc _ f2 _) conn = foldIO update 
                                                       (Right ()) 
                                                       (zip desc (f2 ent))
  where
    update :: SQLResult () -> ((Table, Int, [SQLType]), [SQLValue])
           -> IO (SQLResult ())
    update res ((table, _, _), values) = do
      case res of
        Left err -> return (Left err)
        Right _  -> do
          let (SQLInt key) = (\(x:_) -> x) values
          result <- getColumnNames table conn
          let const = ((col (Column "\"Key\"" ("\"" ++ table ++ "\".\"Key\"")) 
                       .=.
                       (int key)))
          case result of
            Left err -> return (Left err)
            Right columns -> do
              let ((col1,val1):colval) = zip columns values
              execute ("update '" ++ table ++ "' set " ++
                      (foldl
                        (\a (c, v) -> a ++ ", " ++
                        c ++ " = " ++ (valueToString v))
                        (col1 ++ " = " ++ (valueToString val1)) colval) ++
                      " " ++ (trCriteria (Criteria const Nothing)) ++ ";") [] conn

--- Deletes entries depending on wether they fulfill the criteria or not
--- @param en - The EntityDescription descriping the Entities that are to
--- be updated
--- @param const - A Constraint can be be given as input which describes
--- which entities should be deleted. Only entities fulfilling the constraint
--- will be deleted. Nothing deletes every entry.
--- @param conn - A Connection to a database which will be used for this
--- @return A Result without parameter if everything went right, an
--- Error if something went wrong
deleteEntries :: EntityDescription a -> (Maybe Constraint) -> DBAction ()
deleteEntries en const conn = do
  let query = "delete from '" ++ (getTable en) ++ "' " ++
              (case const of
                 Just c -> "where "++(trConstraint c)
                 _      -> "") ++ ";"
  execute query [] conn

--- Drops a table from the database
--- @param en - The EntitiyDescription that describes the entity of which the
--- table should be dropped
--- @param conn -  A Connection to a database which will be used for this
--- @return A Result without paramter if everything went right, an Error if
--- something went wrong
dropTable :: EntityDescription a -> DBAction ()
dropTable en conn = do
  let query = "drop table '" ++ getTable en ++ "';"
  execute query [] conn
  
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
--- @param conn - A Connection to a database which will be used for this
--- @return A Result with a key or an Error if something went wrong.
getLastInsertedKey :: EntityDescription a -> DBAction Int
getLastInsertedKey en conn = do
  let query = "select distinct last_insert_rowid() from '" ++ getTable en ++
                 "';"
  ((select query [] [SQLTypeInt]) >+= 
    (\vals _ -> return $ selectInt vals))
    conn
 where
  selectInt vals = case vals of
    [[key]] -> maybe unknownKeyError
                     Right
                     (Database.CDBI.Description.intOrNothing key)
    _ -> unknownKeyError

  unknownKeyError =
    Left (DBError UnknownError "Key of inserted entity not available")

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
getCondEntries endescr encond =
  getAllEntries endescr >+= \vals _ -> return (Right (filter encond vals))

--- Gets an entry of an entity with a given key.
--- @param endescr   - the EntityDescription describing the entities
--- @param keycolumn - the column containing the primary key
--- @param keyval    - the id-to-value function for entities
--- @param key       - the key of the entity to be fetched
--- @return a DB result with the entry if everything went right,
---         or an error if something went wrong
getEntryWithKey :: Show kid => EntityDescription a -> Column k
                -> (kid -> Value k) -> kid -> DBAction a
getEntryWithKey endescr keycolumn keyval key =
  getEntries All endescr
    (Criteria (equal (colNum keycolumn 0) (keyval key)) Nothing)
    []
    Nothing >+= \vals _ ->
  return $ if null vals then Left keyNotFoundError else Right (head vals)
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
insertNewEntry endescr setkey keycons entity =
  insertEntry entity endescr >+
  getLastInsertedKey endescr >+= \key _ ->
  return (Right (setkey entity (keycons key)))

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
--- @param endescr - the EntityDescription describing the entities to be saved
--- @param dbname - name of the database (e.g. "database.db")
--- @param path   - directory where term file is written
saveDBTerms :: Show a => EntityDescription a -> String -> String -> IO ()
saveDBTerms endescr dbname path = do
  allentries <- runQueryOnDB dbname (getAllEntries endescr)
  let savefile = path </> getTable endescr ++ ".terms"
  if null path
   then putStrLn (unlines (map showQTerm allentries)) -- show only
   else do putStrLn $ "Saving into " ++ savefile
           writeQTermListFile savefile allentries

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
