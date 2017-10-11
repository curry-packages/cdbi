--- ----------------------------------------------------------------------------
--- This module provides datatypes, constructor functions and translation
--- functions to specify SQL criteria including options(group-by, having, order-by)
---
--- @author Mike Tallarek, Julia Krone
--- @version 0.2
--- @category database
--- ----------------------------------------------------------------------------
module Database.CDBI.Criteria (
       Criteria(..), Constraint(..), ColVal(..), GroupBy,
       Option, Value(..), CValue, CColumn, Condition(..), Specifier(..),
       emptyCriteria, int, float, char, string, bool, date,
       col, idVal, colNum, colVal, colValAlt, isNull, isNotNull, equal, 
       (.=.), notEqual, (./=.), greaterThan, (.>.), lessThan, (.<.),
       greaterThanEqual, (.<=.), lessThanEqual, (.>=.), 
       like, (.~.), between, isIn, (.<->.), toCValue, toCColumn,
       ascOrder, descOrder, groupBy, having, groupByCol, trCriteria, 
       trConstraint, trCondition, trValue, trColumn,trSpecifier,
       trOption, sumIntCol, sumFloatCol, countCol, avgIntCol, 
       avgFloatCol, minCol, maxCol, condition, noHave) where

import List (intercalate)
import Time (ClockTime)

import Database.CDBI.Connection  (SQLValue (..), SQLType(..), valueToString)
import Database.CDBI.Description (Column (..), Table)

-- -----------------------------------------------------------------------------
-- Datatypes for constructing SQL criteria
-- -----------------------------------------------------------------------------

--- Criterias for queries that can have a constraint and a group-by clause
data Criteria = Criteria Constraint (Maybe GroupBy)

--- specifier for queries
data Specifier = Distinct | All 

--- datatype to represent order-by statement
data Option = AscOrder CValue 
            | DescOrder CValue
  
--- datatype to represent group-by statement  
data GroupBy = GroupBy CValue GroupByTail

--- subtype for additional columns or having-Clause in group-by statement
data GroupByTail = Having Condition | GBT CValue GroupByTail |  NoHave

--- datatype for conditions inside a having-clause
data Condition = Con Constraint 
               | Fun String Specifier Constraint 
               | HAnd [Condition] 
               | HOr [Condition]
               | Neg Condition
  
--- A datatype to compare values.
--- Can be either a SQLValue or a Column with an additional
--- Integer (rename-number). The Integer is for dealing with renamed
--- tables in queries (i.e. Students as 1Students). If the Integer n is 0
--- the column will be named as usual ("Table"."Column"), otherwise it will
--- be named "nTable"."Column" in the query This is for being able to do
--- complex "where exists" constraints
data Value a = Val SQLValue | Col (Column a) Int 

--- A datatype thats a combination between a Column and a Value
--- (Needed for update queries)
data ColVal = ColVal CColumn CValue

--- Type for columns used inside a constraint.
type CColumn = Column ()

--- Type for values used inside a constraint.
type CValue = Value ()

--- Constraints for queries
--- Every constructor with at least one value has a function as a constructor
--- and only that function will be exported to assure type-safety
--- Most of these are just like the Sql-where-commands Exists needs the
--- table-name, an integer and maybe a constraint
--- (where exists (select * from table where constraint))
--- The integer n will rename the table if it has a different value than 0
--- (where exists (select * from table as ntable where...))
data Constraint
  = IsNull           CValue
  | IsNotNull        CValue
  | BinaryRel RelOp  CValue CValue
  | Between          CValue CValue CValue
  | IsIn             CValue [CValue]
  | Not              Constraint
  | And              [Constraint]
  | Or               [Constraint]
  | Exists           Table Int Constraint
  | None

data RelOp = Eq | Neq | Lt | Lte | Gt | Gte | Like

-- -----------------------------------------------------------------------------
-- Constructor functions
-- -----------------------------------------------------------------------------

--- An empty criteria
emptyCriteria :: Criteria
emptyCriteria = Criteria None Nothing

--- Constructor for a Value Val of type Int
--- @param i - The value
int :: Int -> Value Int
int = Val . SQLInt

--- Constructor for a Value Val of type Float
--- @param i - The value
float :: Float -> Value Float
float = Val . SQLFloat

--- Constructor for a Value Val of type Char
--- @param i - The value
char :: Char -> Value Char
char = Val . SQLChar

--- Constructor for a Value Val of type String
--- @param i - The value
string :: String -> Value String
string = Val . SQLString

--- Constructor for a Value Val of type Bool
--- @param i - The value
bool :: Bool -> Value Bool
bool = Val . SQLBool

--- Constructor for a Value Val of type ClockTime
--- @param i - The value
date :: ClockTime -> Value ClockTime
date = Val . SQLDate

--- Constructor for Values of ID-types
--- Should just be used internally!
idVal :: Int -> Value _
idVal i = Val (SQLInt i)

val :: SQLValue -> Value _
val v = (Val v)

--- Constructor for a Value Col without a rename-number
--- @param c - The column
col :: Column a -> Value a
col c = Col c 0

--- Constructor for a Value Col with a rename-number
--- @param c - The column
--- @param n - The rename-number
colNum :: Column a -> Int -> Value a
colNum c n = Col c n

--- A constructor for ColVal needed for typesafety
--- @param c - The Column
--- @param v - The Value
colVal :: Column a -> Value a -> ColVal
colVal c v = ColVal (toCColumn c) (toCValue v)

--- Alternative ColVal constructor without typesafety
--- @param table - The Tablename of the column
--- @param cl - The column name
--- @param s - The SQLValue
colValAlt :: String -> String -> SQLValue -> ColVal
colValAlt table cl s = 
     ColVal (toCColumn 
            (Column ("\"" ++ cl ++ "\"") 
                    ("\"" ++ table ++ "\".\"" ++ "\"" ++ cl ++ "\""))) 
            (Val s) 

--- IsNull construnctor
--- @param v1 - First Value
isNull :: Value a -> Constraint
isNull v1 = IsNull (toCValue v1)

--- IsNotNull construnctor
--- @param v1 - First Value
isNotNull :: Value a -> Constraint
isNotNull v1 = IsNotNull (toCValue v1)

--- Equal construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
equal :: Value a -> Value a -> Constraint
equal v1 v2 = BinaryRel Eq (toCValue v1) (toCValue v2)

--- Infix Equal
(.=.) :: Value a -> Value a -> Constraint
(.=.) = equal

--- NotEqual construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
notEqual :: Value a -> Value a -> Constraint
notEqual v1 v2 = BinaryRel Neq (toCValue v1) (toCValue v2)

--- Infix NotEqual
(./=.) :: Value a -> Value a -> Constraint
(./=.) = notEqual

--- GreatherThan construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
greaterThan :: Value a -> Value a -> Constraint
greaterThan v1 v2 = BinaryRel Gt (toCValue v1) (toCValue v2)

--- Infix GreaterThan
(.>.) :: Value a -> Value a -> Constraint
(.>.) = greaterThan

--- LessThan construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
lessThan :: Value a -> Value a -> Constraint
lessThan v1 v2 = BinaryRel Lt (toCValue v1) (toCValue v2)

--- Infix LessThan
(.<.) :: Value a -> Value a -> Constraint
(.<.) = lessThan

--- GreaterThanEqual construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
greaterThanEqual :: Value a -> Value a -> Constraint
greaterThanEqual v1 v2 = BinaryRel Gte (toCValue v1) (toCValue v2)

--- Infix GreaterThanEqual
(.>=.) :: Value a -> Value a -> Constraint
(.>=.) = greaterThanEqual

--- LessThanEqual construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
lessThanEqual :: Value a -> Value a -> Constraint
lessThanEqual v1 v2 = BinaryRel Lte (toCValue v1) (toCValue v2)

--- Infix LessThanEqual
(.<=.) :: Value a -> Value a -> Constraint
(.<=.) = lessThanEqual

--- Like construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
like :: Value a -> Value a -> Constraint
like v1 v2 = BinaryRel Like (toCValue v1) (toCValue v2)

--- Infix Like
(.~.) :: Value a -> Value a -> Constraint
(.~.) = like

--- Between construnctor
--- @param v1 - First Value
--- @param v2 - Second Value
--- @param v3 - Third Value
between :: Value a -> Value a -> Value a -> Constraint
between v1 v2 v3 = Between (toCValue v1) (toCValue v2) (toCValue v3)

--- IsIn construnctor
--- @param v1 - First Value
--- @param xs - List of Values
isIn :: Value a -> [Value a] -> Constraint
isIn v1 xs = IsIn (toCValue v1) (map toCValue xs)

--- Infix IsIn
(.<->.) :: Value a -> [Value a] -> Constraint
(.<->.) = isIn

--- Constructor for the option: Ascending Order by Column
--- @param c - The Column that should be ordered by
ascOrder :: Value a -> Option
ascOrder v = AscOrder (toCValue v)

--- Constructor for the option: Descending Order by Column
--- @param c - The Column that should be ordered by
descOrder :: Value a -> Option
descOrder v = DescOrder (toCValue v)

---Constructor for group-by-clause
---@param c - The Column that should be grouped by
---@param gbTail - GroupByTail, i.e. more columns or a having-clause
groupBy :: Value a -> GroupByTail -> GroupBy
groupBy c gbTail = GroupBy (toCValue c) gbTail

--- Constructor to specifiy more than one column for group-by
--- @param c - the additional column
--- @param gbTail - subsequent part of group-by statement
groupByCol :: Value a -> GroupByTail -> GroupByTail
groupByCol c gbTail = GBT (toCValue c) gbTail

---Constructor for having condition
---@param con - The condition
having :: Condition -> GroupByTail
having con = Having con

--- Constructor for empty having-Clause
noHave :: GroupByTail
noHave = NoHave

---Constructor for Condition with just a simple Constraint
condition :: Constraint -> Condition
condition con = Con con

---Constructor for aggregation function sum for columns of type Int 
--- having-clauses.
---@param spec - specifier Distinct or All
---@param c - Column that is to be summed up
---@param v - value the result is compared to (has to be of type Int)
---@param op - relating operator
sumIntCol :: Specifier -> Value Int -> Value Int 
                       -> (Value () -> Value () -> Constraint) -> Condition   
sumIntCol spec c v op =  (Fun "Sum " spec (op (toCValue c) (toCValue v)))

--- Constructor for aggregation function sum for columns of type float
--- in having-clauses.
---@param spec - specifier Distinct or All
---@param c - Column that is to be summed up
---@param v - value the result is compared to (has to be of type float)
---@param op - relating operator
sumFloatCol :: Specifier -> Value Float -> Value Float 
                       -> (Value () -> Value () -> Constraint) -> Condition   
sumFloatCol spec c v op =  (Fun "Sum " spec (op (toCValue c) (toCValue v)))

--- Constructor for aggregation function avg for columns of type Int 
--- in having-clauses.
---@param spec - specifier Distinct or All
---@param c - Column that is to be averaged
---@param v - value the result is compared to (has to be of type float)
---@param op - relating operator
avgIntCol :: Specifier -> Value Int -> Value Float
                       -> (Value () -> Value () -> Constraint) -> Condition 
avgIntCol spec c v op =  (Fun "Avg " spec (op (toCValue c) (toCValue v)))

--- Constructor for aggregation function avg for columns of type float
--- in having-clauses.
---@param spec - specifier Distinct or All
---@param c - Column that is to be avaraged
---@param v - value the result is compared to (has to be of type float)
---@param op - relating operator
avgFloatCol :: Specifier -> Value Float -> Value Float
                       -> (Value () -> Value () -> Constraint) -> Condition 
avgFloatCol spec c v op =  (Fun "Avg " spec (op (toCValue c) (toCValue v)))

---Constructor for aggregation function count in having-clauses.
---@param spec - specifier Distinct or All
---@param c - Column which elements are to be counted
---@param v - value the result is compared to (has to be of type Int).
---@param op - relating operator
countCol :: Specifier -> Value _ -> Value Int 
                       -> (Value () -> Value () -> Constraint) -> Condition 
countCol spec c v op = (Fun "Count " spec (op (toCValue c) (toCValue v)))

--- Constructor for aggregation function min in having-clauses.
--- @param spec - specifier Distinct or All
---@param c - column the minimal element has to be extracted from
---@param v - value to compare to the minimal value, same type as column
---@param op - operator 
minCol :: Specifier -> Value a -> Value a 
                       -> (Value () -> Value () -> Constraint) -> Condition 
minCol spec c v op = (Fun "Min " spec (op (toCValue c) (toCValue v)))

--- Constructor for aggregation function max in having-clauses.
--- @param spec - specifier Distinct or All
---@param c - column the maximal element has to be extracted from
---@param v - value to compare to the maximal value, same type as column
---@param op - operator 
maxCol :: Specifier -> Value a -> Value a 
                       -> (Value () -> Value () -> Constraint) -> Condition
maxCol spec c v op = (Fun "Max " spec (op (toCValue c) (toCValue v)))

--Convert to UnitType
toCColumn :: Column a -> Column ()
toCColumn (Column s1 s2) = (Column s1 s2)

toCValue :: Value a -> CValue
toCValue (Col (Column s1 s2) n) = Col (Column s1 s2) n
toCValue (Val               v1) = Val v1


-- -----------------------------------------------------------------------------
-- Translation of a constraint into a SQL string.
-- -----------------------------------------------------------------------------

-- Translate Criteria to a string in a sql-query
trCriteria :: Criteria -> String
trCriteria crit = case crit of
  (Criteria None group)  -> trGroup group
  (Criteria c group)     -> " where " ++ trConstraint c ++ " "
                            ++ trGroup group

-- Translate an Order-by to a string in a sql-query
trOption :: [Option] -> String
trOption []       = ""
trOption (ls@((AscOrder _):_)) = " order by " ++ 
                                     intercalate ", " (map trOption' ls)
trOption (ls@((DescOrder _):_)) = " order by " ++ 
                                     intercalate ", " (map trOption' ls)

trOption' :: Option -> String
trOption' (AscOrder  v) = trValue v ++ " asc"
trOption' (DescOrder v) = trValue v ++ " desc"
 
-- translate a group-by to a string in a sql-query                                    
trGroup :: (Maybe GroupBy) -> String
trGroup Nothing = ""
trGroup (Just (GroupBy cs gbTail)) = " group by " ++ trValue cs ++ 
                                        trTail gbTail 

trTail :: GroupByTail -> String
trTail (GBT cs gbTail) = ", "++ trValue cs ++ trTail gbTail
trTail NoHave = ""
trTail (Having cond) = " Having " ++ trCondition cond

trCondition :: Condition -> String      
trCondition (HAnd conds) = intercalate " and " (map trCondition conds)
trCondition (HOr conds) = intercalate " or " (map trCondition conds)
trCondition (Con cons) = trConstraint cons
trCondition (Neg cond) = "(not "++ (trCondition cond)++")"
trCondition (Fun fun spec cons) = "("++fun ++ "("++trSpecifier spec ++constr
  where ('(':'(':constr) = trConstraint cons 


-- Translate a Constraint to a string in a sql-query
trConstraint :: Constraint -> String
trConstraint (IsNull            v) = paren $ (paren $ trValue v) ++ " is NULL"
trConstraint (IsNotNull         v) = paren $ (paren $ trValue v) ++ " is not NULL"
trConstraint (BinaryRel rel v1 v2)
  = paren $ (paren $ trValue v1) ++ trRelOp rel ++ trValue v2
trConstraint (Between    v1 v2 v3)
  = paren $ (paren $ trValue v1) ++ " between " ++ trValue v2 ++ " and " ++ trValue v3
trConstraint (IsIn           v vs)
  = paren $ trValue v ++ " in " ++ paren (intercalate ", " $ map trValue vs)
trConstraint (Not               c) = paren $  "not " ++ trConstraint c
trConstraint (And []      ) = ""
trConstraint (And cs@(_:_)) = paren $ intercalate " and " (map trConstraint cs)
trConstraint (Or []       ) = ""
trConstraint (Or  cs@(_:_)) = paren $ intercalate " or " (map trConstraint cs)
trConstraint (Exists table n cs) =
  "(exists (select * from '" ++ table ++ "' " ++
  (asTable table n) ++ " " ++ (trCriteria (Criteria cs Nothing)) ++ "))"

trRelOp :: RelOp -> String
trRelOp Eq   = " == "
trRelOp Neq  = " <> "
trRelOp Lt   = " < "
trRelOp Lte  = " <= "
trRelOp Gt   = " > "
trRelOp Gte  = " >= "
trRelOp Like = " like "

-- Translate a Value to a String
trValue :: Value a -> String
trValue (Val              v) = valueToString v
trValue (Col (Column _ c) n) = trColumn c n

-- Translate "Table.Column" to "nTable.Colum" where n is the Integer-Parameter
trColumn :: String -> Int -> String
trColumn ('"':table_column) n =
  if n==0 then '"' : table_column
          else '"' : show n ++ table_column
{-
trColumn ("\"" ++ table ++ "\"." ++ column) n =
    case n of
        0 -> "\"" ++ table ++ "\"." ++ column
        m -> "\"" ++ (show m) ++ table ++ "\"." ++ column
-}

paren :: String -> String
paren s = '(' : s ++ ")"

-- Create the "as tablename" string
asTable :: Table -> Int -> Table
asTable table n = case n of
                      0 -> ""
                      m -> " as '" ++ (show m) ++ table ++ "'"

--Translate specifier
trSpecifier :: Specifier -> String
trSpecifier All = ""
trSpecifier Distinct = "Distinct "