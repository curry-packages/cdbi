--- --------------------------------------------------------------
--- This module contains datatype declarations, constructor functions
--- selectors and translation functions for complex select queries  
--- in particular for those selecting (1 to 5) single columns.
--- @author Julia Krone
---
--- @version 0.1
--- @category database
--- ----------------------------------------------------------------

module Database.CDBI.QueryTypes(
   SetOp(..),Join(..),innerJoin, crossJoin,
  ColumnSingleCollection(..), ColumnTupleCollection, 
  ColumnTripleCollection, ColumnFourTupleCollection,
  ColumnFiveTupleCollection, 
  sum, avg, count, minV, maxV, none,
  caseThen, singleCol, tupleCol, tripleCol, fourCol, fiveCol,
  SingleColumnSelect(..), TupleColumnSelect(..),
  TripleColumnSelect(..), FourColumnSelect(..),
  FiveColumnSelect(..), TableClause(..),
  getSingleType, getTupleTypes, getTripleTypes, getFourTupleTypes,
  getFiveTupleTypes, getSingleValFunc, getTupleValFuncs, getTripleValFuncs,
  getFourTupleValFuncs, getFiveTupleValFuncs,
  trLimit, trSpecifier, trSetOp, trSingleSelectQuery, asTable,
  trTupleSelectQuery, trJoinPart1, trJoinPart2, trTripleSelectQuery,
  trFourTupleSelectQuery, trFiveTupleSelectQuery,
  caseResultInt, caseResultFloat, caseResultString, caseResultChar, 
  caseResultBool)
 where

import List(intercalate)
import Time(ClockTime)

import Database.CDBI.Connection
import Database.CDBI.Criteria
import Database.CDBI.Description

---datatype for set operations
data SetOp = Union | Intersect | Except 

--- datatype for joins
data  Join =  Cross
           | Inner Constraint
    
  
--- Constructorfunction for an inner join
---@param constraint
innerJoin :: Constraint -> Join
innerJoin constraint = Inner constraint

--- Constructorfunction for cross join
crossJoin :: Join
crossJoin = Cross

--- data structure to represent a table-clause (tables and joins)
--- in a way that at least one table has to be specified
data TableClause = TC Table Int (Maybe (Join,TableClause))

-- Data type to specify the result type of case expressions and its conversion.
type CaseVal a = (SQLType , (SQLValue -> a))

--- Datatype representing a single column in a select-clause. Can be just a 
--- column connected with an alias and an optional aggregation function(String)
--- or a Case-when-then-statement
data ColumnSingleCollection a = ResultColumnDescription (ColumnDescription a)  
                                                        Int  
                                                        String 
                              | Case Condition (CValue, CValue) (CaseVal a)

--- Datatype to select two different columns which can be of different types 
--- and from different tables.
type ColumnTupleCollection a b = (ColumnSingleCollection a, 
                                 ColumnSingleCollection b)

--- Datatype to select three different columns which can be of different types 
--- and from different tables.
type ColumnTripleCollection a b c = (ColumnSingleCollection a, 
                                     ColumnSingleCollection b, 
                                     ColumnSingleCollection c)

--- Datatype to select four different columns which can be of different types 
--- and from different tables.
type ColumnFourTupleCollection a b c d = (ColumnSingleCollection a,
                                          ColumnSingleCollection b, 
                                          ColumnSingleCollection c, 
                                          ColumnSingleCollection d)

--- Datatype to select five different columns which can be of different types 
--- and from different tables.
type ColumnFiveTupleCollection a b c d e= (ColumnSingleCollection a, 
                                           ColumnSingleCollection b, 
                                           ColumnSingleCollection c, 
                                           ColumnSingleCollection d, 
                                           ColumnSingleCollection e)

-- Data type to represent an aggregation function in a select-clause.                                           
type Fun a = (String , ColumnDescription a) 

--- Constructor for aggregation function sum
--- in select-clauses.
--- A pseudo-ResultColumnDescription of type 
--- float is created for correct return type.
sum :: Specifier -> ColumnDescription _ -> Fun Float
sum spec (ColDesc name _ _ _) = 
  ("Sum( "++ (trSpecifier spec) , 
  (ColDesc name SQLTypeFloat (\f -> (SQLFloat f)) getFloatValue))

--- Constructor for aggregation function avg 
--- in select-clauses.
--- A pseudo-ResultColumnDescription of type
--- float is created for correct return type.
avg :: Specifier -> ColumnDescription _ -> Fun Float
avg spec (ColDesc name _ _ _) = 
  ("Avg( "++ (trSpecifier spec), 
  (ColDesc name SQLTypeFloat (\f -> (SQLFloat f)) getFloatValue))

--- Constructor for aggregation function count 
--- in select-clauses.
--- A pseudo-ResultColumnDescription of type 
--- float is created for correct return type.
count :: Specifier -> ColumnDescription _ -> Fun Int
count spec (ColDesc name _ _ _) = 
  ("Count( "++ (trSpecifier spec), 
  (ColDesc name SQLTypeInt (\i -> (SQLInt i)) getIntValue))

--- Constructor for aggregation function min in select-clauses.
minV :: ColumnDescription a -> Fun a
minV cd = ("Min( ", cd)

--- Constructor for aggregation function max in select-clauses.
maxV :: ColumnDescription a -> Fun a
maxV cd = ("Max( ", cd)

--- Constructor function in case no aggregation function is specified.
none :: ColumnDescription a -> Fun a
none cd = ("(", cd)

---Constructor for CaseVal of type Int 
---expecting result of type Int in case-expression
caseResultInt :: CaseVal Int
caseResultInt = (SQLTypeInt, getIntValue)

---Constructor for CaseVal of type Float
---expecting result of type Float in case-expression
caseResultFloat :: CaseVal Float
caseResultFloat = (SQLTypeFloat, getFloatValue)

---Constructor for CaseVal of type String
---expecting result of type String in case-expression
caseResultString :: CaseVal String
caseResultString = (SQLTypeString, getStringValue)

---Constructor for CaseVal of type Date 
---expecting result of type Date in case-expression
caseResultDate :: CaseVal Time.ClockTime
caseResultDate = (SQLTypeDate, getDateValue)

---Constructor for CaseVal of type Bool 
---expecting result of type Bool in case-expression
caseResultBool :: CaseVal Bool
caseResultBool = (SQLTypeBool, getBoolValue)

---Constructor for CaseVal of type Char 
---expecting result of type Char in case-expression
caseResultChar :: CaseVal Char
caseResultChar = (SQLTypeChar, getCharValue)

--- Constructor function for representation of statement: 
--- CASE WHEN condition THEN val1 ELSE val2 END.
--- It does only work for the same type in then and
--- else branch.
---@param con - the condition
---@param val1 - value for then-branch
---@param val2 - value for else-branch
---@param cv - data providing SQLType and conversion function                           
caseThen :: Condition -> Value a -> Value a -> (CaseVal a) 
                        -> ColumnSingleCollection a
caseThen con val1 val2 cv = 
  Case con ((toCValue val1), (toCValue val2)) cv 

--- Constructorfunction for ColumnSingleCollection.
---@param coldecs - ColumnDescription of column to select
---@param alias - alias of the table
---@param f - aggregation function (constructor)
singleCol :: ColumnDescription a -> Int -> (ColumnDescription a -> Fun b) 
                                        -> ColumnSingleCollection b
singleCol colDesc alias f = ResultColumnDescription convColDesc alias fun
  where (fun, convColDesc) = f colDesc

---Constructor function for ColumnTupleCollection.
---@param col1 - first ColumnSingleCollection
---@param col2 - second ColumnSingleCollection
tupleCol :: ColumnSingleCollection a -> ColumnSingleCollection b 
                                     -> ColumnTupleCollection a b
tupleCol col1 col2 = (col1,col2)

---Constructor function for ColumnTripleCollection.
tripleCol :: ColumnSingleCollection a
          -> ColumnSingleCollection b
          -> ColumnSingleCollection c
          -> ColumnTripleCollection a b c
tripleCol col1 col2 col3 = (col1, col2, col3)

---Constructor function for ColumnFourTupleCollection.
fourCol :: ColumnSingleCollection a
        -> ColumnSingleCollection b
        -> ColumnSingleCollection c
        -> ColumnSingleCollection d
        -> ColumnFourTupleCollection a b c d
fourCol col1 col2 col3 col4 = (col1, col2, col3, col4)

---Constructor function for ColumnFiveTupleCollection.
fiveCol :: ColumnSingleCollection a
        -> ColumnSingleCollection b
        -> ColumnSingleCollection c
        -> ColumnSingleCollection d
        -> ColumnSingleCollection e
        -> ColumnFiveTupleCollection a b c d e
fiveCol col1 col2 col3 col4 col5 = (col1, col2, col3, col4, col5)

--- Datatype to describe all parts of a select-query without Setoperators 
--- order-by and limit (selecthead) for a single column.   
data SingleColumnSelect a  = SingleCS Specifier 
                                      (ColumnSingleCollection a)  
                                      TableClause 
                                      Criteria

--- Datatype to describe all parts of a select-query without Setoperators 
--- order-by and limit (selecthead) for two columns.   
data TupleColumnSelect a b = TupleCS  Specifier 
                                      (ColumnTupleCollection a b) 
                                      TableClause 
                                      Criteria

--- Datatype to describe all parts of a select-query without Setoperators 
--- order-by and limit (selecthead) for three columns. 
data TripleColumnSelect a b c = TripleCS Specifier
                                         (ColumnTripleCollection a b c)
                                         TableClause
                                         Criteria

--- Datatype to describe all parts of a select-query without Setoperators 
--- order-by and limit (selecthead) for four columns. 
data FourColumnSelect a b c d = FourCS Specifier
                                       (ColumnFourTupleCollection a b c d)
                                       TableClause
                                       Criteria

--- Datatype to describe all parts of a select-query without Setoperators 
--- order-by and limit (selecthead) for five columns. 
data FiveColumnSelect a b c d e = FiveCS Specifier
                                         (ColumnFiveTupleCollection a b c d e)
                                         TableClause
                                         Criteria

--selector of the SQLType encasulated in a ColumnSingleCollection                                         
getColumnType :: ColumnSingleCollection _ -> [SQLType]
getColumnType col = 
  case col of
       (ResultColumnDescription (ColDesc _ typ _ _) _ _) -> [typ]
       (Case _ (_ , _) (typ,_))                          -> [typ]
 
-- selector of the conversion function encapsulated in a 
-- ColumnSingleCollection
getColumnValFunc :: ColumnSingleCollection a -> (SQLValue -> a)
getColumnValFunc col =
  case col of
       (ResultColumnDescription (ColDesc _ _ _ f) _ _)   -> f
       (Case _ (_, _) (_,f))                             -> f
                                       
--selector: returns the SQLType of the ColumnSingleCollection
-- inside of SingleColumnSelect
getSingleType :: SingleColumnSelect _ -> [SQLType]
getSingleType (SingleCS _ col _ _) = getColumnType col
  
--selector: returns the function to convert the SQLValue to the resulttype
getSingleValFunc :: SingleColumnSelect a -> (SQLValue -> a)
getSingleValFunc (SingleCS _ col _ _) = getColumnValFunc col                    

--selector: returns the list of SQLTypes used
-- inside the TupleColumnSelect
getTupleTypes :: TupleColumnSelect _ _ -> [SQLType]
getTupleTypes (TupleCS _ (col1,col2) _ _) = getColumnType col1
                                              ++ getColumnType col2

--selector: returns the tuple of functions to convert the SQLValue 
--to the resulttype
getTupleValFuncs :: TupleColumnSelect a b -> ((SQLValue -> a), (SQLValue -> b))
getTupleValFuncs (TupleCS _ (col1,col2) _ _) = (getColumnValFunc col1,
                                                  getColumnValFunc col2)

--selector: returns the list of SQLTypes used
-- inside the TripleColumnSelect
getTripleTypes :: TripleColumnSelect _ _ _ -> [SQLType]
getTripleTypes (TripleCS _ (col1, col2, col3) _ _) =
  getColumnType col1 ++ getColumnType col2 ++ getColumnType col3

--selector: returns the triple of functions to convert the SQLValue 
--to the resulttype
getTripleValFuncs :: TripleColumnSelect a b c -> 
                            ((SQLValue -> a), (SQLValue -> b), (SQLValue -> c))
getTripleValFuncs (TripleCS _ (col1, col2, col3) _ _) = 
  (getColumnValFunc col1, getColumnValFunc col2, getColumnValFunc col3)

--selector: returns the list of SQLTypes used
-- inside the FourColumnSelect
getFourTupleTypes :: FourColumnSelect _ _ _ _ -> [SQLType]
getFourTupleTypes (FourCS _ (col1, col2, col3, col4) _ _) =
  getColumnType col1 ++ getColumnType col2 ++ 
    getColumnType col3 ++ getColumnType col4

--selector: returns the fourtuple of functions to convert the 
--SQLValue to the resulttype
getFourTupleValFuncs :: FourColumnSelect a b c d ->
                           ((SQLValue -> a), (SQLValue -> b), 
                             (SQLValue -> c), (SQLValue -> d))
getFourTupleValFuncs (FourCS _ (col1, col2, col3, col4) _ _) =
  (getColumnValFunc col1, getColumnValFunc col2, 
     getColumnValFunc col3, getColumnValFunc col4)


--selector: returns the list of SQLTypes used
-- inside the FiveColumnSelect
getFiveTupleTypes :: FiveColumnSelect a b c d e -> [SQLType]
getFiveTupleTypes (FiveCS _ (col1, col2, col3, col4, col5) _ _) = 
  getColumnType col1 ++ getColumnType col2 ++ 
    getColumnType col3 ++ getColumnType col4 ++ getColumnType col5

--selector: returns the fivetuple of functions to convert the 
--SQLValue to the resulttype
getFiveTupleValFuncs :: FiveColumnSelect a b c d e -> ((SQLValue -> a),
  (SQLValue -> b), (SQLValue -> c), (SQLValue -> d), (SQLValue -> e))
getFiveTupleValFuncs (FiveCS _ (col1, col2, col3, col4, col5) _ _) = 
  (getColumnValFunc col1, getColumnValFunc col2, getColumnValFunc col3,
    getColumnValFunc col4, getColumnValFunc col5)

-- ------------------------------------------------------------------------------
-- translation functions
-- ------------------------------------------------------------------------------

-- Transform a SingleColumnSelect to its string representation.
trSingleSelectQuery :: SingleColumnSelect _ -> String
trSingleSelectQuery (SingleCS sp col tabs crit) =
  ("select "++ trSpecifier sp ++ getResultColumnString col ++" from " ++
   (getTableString tabs "") ++ " " ++ trCriteria crit)

-- Transform a TupleColumnSelect to its string representation.
trTupleSelectQuery :: TupleColumnSelect _ _ -> String
trTupleSelectQuery (TupleCS sp (col1, col2) tabs crit) =
  ("select " ++trSpecifier sp ++ getResultColumnString col1 ++ ", "++ 
    getResultColumnString col2 ++" from " ++ (getTableString tabs "") ++ 
      trCriteria crit )

-- Transform a TripleColumnSelect to its string representation.
trTripleSelectQuery :: TripleColumnSelect _ _ _ -> String
trTripleSelectQuery (TripleCS sp (col1, col2, col3) tabs crit) =
  ("select " ++trSpecifier sp ++ getResultColumnString col1 ++ ", "++ 
    getResultColumnString col2 ++ ", "++  getResultColumnString col3 ++
     " from " ++ (getTableString tabs "") ++ trCriteria crit )

-- Transform a FourTupleColumnSelect to its string representation.
trFourTupleSelectQuery :: FourColumnSelect _ _ _ _ -> String
trFourTupleSelectQuery (FourCS sp (col1, col2, col3, col4) tabs crit) =
  ("select " ++trSpecifier sp ++ getResultColumnString col1 ++ ", "++ 
    getResultColumnString col2 ++ ", "++  getResultColumnString col3 
      ++ ", "++  getResultColumnString col4 ++ " from " ++
      (getTableString tabs "") ++ trCriteria crit )

-- Transform a FiveTupleColumnSelect to its string representation.
trFiveTupleSelectQuery :: FiveColumnSelect _ _ _ _ _ -> String
trFiveTupleSelectQuery (FiveCS sp (col1, col2, col3, col4, col5) tabs crit) =
  ("select " ++trSpecifier sp ++ getResultColumnString col1 ++ ", "++ 
    getResultColumnString col2 ++", "++ getResultColumnString col3 
      ++", "++  getResultColumnString col4 ++ ", "++  getResultColumnString col5
        ++" from " ++ (getTableString tabs "") ++ trCriteria crit )

-- translate set operations        
trSetOp :: SetOp -> String
trSetOp Union = " union "
trSetOp Intersect = " intersect "
trSetOp Except = " except "

-- translate limit clause
trLimit :: Maybe Int -> String
trLimit limit = case limit of
                     Nothing -> ""
                     Just n -> " Limit "++(show n)

-- Create the "as tablename" string
asTable :: Table -> Int -> Table
asTable table n = case n of
                      0 -> ""
                      m -> " as '" ++ (show m) ++ table ++ "'"

-- translate joins                      
trJoinPart1 :: Join -> String
trJoinPart1 Cross = " cross join"
trJoinPart1 (Inner _) = " inner join"
     
trJoinPart2 :: Join -> String     
trJoinPart2 Cross = ""
trJoinPart2 (Inner constraint) = 
                          " ON (" ++ (trConstraint constraint)++")"

-- translate a ColumnSingleCollection                          
getResultColumnString:: ColumnSingleCollection a -> String
getResultColumnString (ResultColumnDescription (ColDesc name _ _ _) alias aggr)
                                          = aggr ++ (trColumn name alias) ++ ")"
getResultColumnString (Case con (val1, val2) _ ) 
               = "( case when " ++ trCondition con ++ " then " ++ trValue val1 
                                        ++ " else " ++ trValue val2 ++ " end)"

-- translate a table-clause                                        
getTableString :: TableClause -> String -> String
getTableString (TC tab alias Nothing) join2 = 
   (" '" ++tab ++ "'" ++ (asTable tab alias)++" "++join2)
getTableString (TC tab alias (Just (join, tc))) join2 = 
     (" '" ++tab ++ "'" ++ (asTable tab alias)++join2
       ++(trJoinPart1 join) ++ (getTableString tc (trJoinPart2 join)))
              

getCharValue :: SQLValue -> Char  
getCharValue (SQLChar char) = char

getDateValue :: SQLValue -> Time.ClockTime
getDateValue (SQLDate date) = date

getFloatValue :: SQLValue -> Float
getFloatValue (SQLFloat float) = float

getIntValue :: SQLValue -> Int
getIntValue (SQLInt int) = int

getStringValue :: SQLValue -> String
getStringValue (SQLString str) = str

getBoolValue :: SQLValue -> Bool
getBoolValue (SQLBool bool) = bool
