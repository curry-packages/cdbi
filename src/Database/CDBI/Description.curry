--- This module contains basic datatypes and operations to represent
--- a relational data model in a type-safe manner. This representation is
--- used by the library `Database.CDBI.ER` to provide type safety
--- when working with relational databases.
--- The tool `erd2cdbi` generates from an entity-relationship model
--- a Curry program that represents all entities and relationships
--- by the use of this module.
---
--- @author Mike Tallarek, changes by Julia Krone
--- @version 0.2
--- @category database
--- ----------------------------------------------------------------------------
module Database.CDBI.Description where

import Time

import Database.CDBI.Connection (SQLType, SQLValue(..))

-- -----------------------------------------------------------------------------
-- Datatypes for describing entities
-- -----------------------------------------------------------------------------

--- The datatype EntityDescription is a description of a database entity
--- type including the name, the types the entity consists of, a function
--- transforming an instance of this entity to a list of SQLValues, a
--- second function doing the same but converting the key value always to
--- SQLNull to ensure that keys are auto incrementing and a
--- function transforming a list of SQLValues to an instance of this entity
data EntityDescription a = ED String 
                              [SQLType] 
                              (a -> [SQLValue]) 
                              (a -> [SQLValue]) --for insertion
                              ([SQLValue] -> a)

  
--- Entity-types can be combined (For Example Student and Lecture could be
--- combined to Data StuLec = StuLec Student Lecture). If a description for
--- this new type is written CDBI can look up that type in the database
--- The description is a list of Tuples consisting of a String (The name of
--- the entity type that will be combined), a "rename-number" n which will
--- rename the table to "table as ntable" and a list of SQLTypes (The types
--- that make up that entity type).  Furthermore there has to be a function
--- that transform a list of SQLValues into this combined type, and two
--- functions that transform the combined type into a list of SQLValues, the 
--- first one for updates, the second one for insertion.
--- The list of sqlvalues needs to match what is returned by the database.
data CombinedDescription a = CD [(Table, Int, [SQLType])] 
                                ([SQLValue] -> a) 
                                (a -> [[SQLValue]]) 
                                (a -> [[SQLValue]]) -- for insertion

--- A type representing tablenames
type Table = String

--- A datatype representing column names.
--- The first string is the simple name of the column (for example the
--- column Name of the row Student). The second string is the name of the
--- column combined with the name of the row (for example Student.Name).
--- These names should always be in quotes (for example "Student"."Name")
--- so no errors emerge (the name "Group" for
--- example would result in errors if not in quotes).
--- Has a phantom-type for the value the column represents.
data Column _ = Column String String

--- Datatype representing columns for selection.
--- This datatype has to be distinguished from type Column which is just for 
--- definition of conditions.
--- The type definition consists of the complete name (including tablename),
--- the SQLType of the column
--- and two functions for the mapping from SQLValue into the resulttype and 
--- the other way around
data ColumnDescription a = ColDesc String 
                                   SQLType 
                                   (a -> SQLValue) 
                                   (SQLValue -> a)

--- A constructor for CombinedDescription.
--- @param ed1 - Description of the first Entity-Type that is to be combined
--- @param rename1 - The "rename-number" for ed1. If it is zero ed1 will
--- not be renamed in queries, otherwise is will be renamed as
--- follows: "table as ntable"
--- @param ed2 - Description of the second Entity-Type that is to be combined
--- @param rename2 - Same as rename1 for ed2
--- @param f - A function that describes how the combined entity is built.
--- Takes two entities that make up the combined entity as parameters
--- and combines those into the combined entity.
combineDescriptions :: EntityDescription a -> 
                       Int -> 
                       EntityDescription b -> 
                       Int -> 
                       (a -> b -> c) -> 
                       (c -> (a, b)) -> 
                       CombinedDescription c
combineDescriptions ed1 rename1 ed2 rename2 f1 f2 =
  CD [(getTable ed1, rename1, getTypes ed1),
      (getTable ed2, rename2, getTypes ed2)] 
     createFunction1 createFunction2 createFunction3
    where createFunction1 xs = f1 ((getToEntity ed1) 
                                   (take lengthEd1 xs))
                                  ((getToEntity ed2) 
                                   (drop lengthEd1 xs))
             where lengthEd1 = length (getTypes ed1)
          createFunction2 combEnt = 
            let (ent1, ent2) = f2 combEnt in
              ((getToValues ed1) ent1) : [(getToValues ed2) ent2]
          createFunction3 combEnt = 
            let (ent1, ent2) = f2 combEnt in
              ((getToInsertValues ed1) ent1) : [(getToInsertValues ed2) ent2]

--- Adds another ED to an already existing CD.
--- @param ed1 - The ED to be added
--- @param rename - The "rename-number"
--- @param f1 - A function that describes how the combined entity is built.
--- Takes the entity that should be added and the combined entity as parameter
--- and combines those into a new version of the combined entity.
--- @param cd - The already existing CD
addDescription :: EntityDescription a -> 
                  Int -> 
                  (a -> b -> b) -> 
                  (b -> a) -> 
                  CombinedDescription b -> 
                  CombinedDescription b
addDescription ed1 rename f1 f2 (CD xs f1' f2' f3') =
  CD ((getTable ed1, rename, getTypes ed1) : xs) 
     createFunction1 
     createFunction2
     createFunction3
    where createFunction1 ys = 
            f1 ((getToEntity ed1) 
               (take lengthEd1 ys)) 
               (f1' (drop lengthEd1 ys))
            where lengthEd1 = length (getTypes ed1)
          createFunction2 combEnt = 
            [(getToValues ed1) (f2 combEnt)] ++ (f2' combEnt)
          createFunction3 combEnt = 
            [(getToInsertValues ed1) (f2 combEnt)] ++ (f3' combEnt)


-- -----------------------------------------------------------------------------
-- Auxiliary Functions
-- -----------------------------------------------------------------------------

getTable :: EntityDescription a -> String
getTable (ED s _ _ _ _) = s

getTypes :: EntityDescription a -> [SQLType]
getTypes (ED _ t _ _ _) = t

getToValues :: EntityDescription a -> (a -> [SQLValue])
getToValues (ED _ _ f _ _) = f

getToInsertValues :: EntityDescription a -> (a -> [SQLValue])
getToInsertValues (ED _ _ _ f _) = f

getToEntity :: EntityDescription a -> ([SQLValue] -> a)
getToEntity (ED _ _ _ _ f) = f

getColumnSimple :: Column a -> String
getColumnSimple (Column s _ ) = s

getColumnFull :: Column a -> String
getColumnFull (Column _ s ) = s

getColumnName :: ColumnDescription a -> String
getColumnName (ColDesc s _ _ _) = s

getColumnTableName :: ColumnDescription a -> String
getColumnTableName (ColDesc s _ _ _) = s

getColumnTyp :: ColumnDescription a -> SQLType
getColumnTyp (ColDesc _ t _ _) = t

getColumnValueBuilder :: ColumnDescription a -> (a -> SQLValue)
getColumnValueBuilder (ColDesc _ _ f _) = f

getColumnValueSelector :: ColumnDescription a -> (SQLValue -> a)
getColumnValueSelector (ColDesc _ _ _ f) = f

-- Conversion functions
toValueOrNull :: (a -> SQLValue) -> Maybe a -> SQLValue
toValueOrNull _ Nothing  = SQLNull
toValueOrNull f (Just v) = f v

sqlIntOrNull :: (Maybe Int) -> SQLValue
sqlIntOrNull Nothing = SQLNull
sqlIntOrNull (Just a) = SQLInt a

sqlFloatOrNull :: (Maybe Float) -> SQLValue
sqlFloatOrNull Nothing = SQLNull
sqlFloatOrNull (Just a) = SQLFloat a

sqlCharOrNull :: (Maybe Char) -> SQLValue
sqlCharOrNull Nothing = SQLNull
sqlCharOrNull (Just a) = SQLChar a

sqlStringOrNull :: (Maybe String) -> SQLValue
sqlStringOrNull Nothing = SQLNull
sqlStringOrNull (Just a) = SQLString a

sqlString :: String -> SQLValue
sqlString a = SQLString a

sqlBoolOrNull :: (Maybe Bool) -> SQLValue
sqlBoolOrNull Nothing = SQLNull
sqlBoolOrNull (Just a) = SQLBool a

sqlDateOrNull :: (Maybe ClockTime) -> SQLValue
sqlDateOrNull Nothing = SQLNull
sqlDateOrNull (Just a) = SQLDate a

intOrNothing :: SQLValue -> (Maybe Int)
intOrNothing SQLNull = Nothing
intOrNothing (SQLInt a) = Just a

floatOrNothing :: SQLValue -> (Maybe Float)
floatOrNothing SQLNull = Nothing
floatOrNothing (SQLFloat a) = Just a

charOrNothing :: SQLValue -> (Maybe Char)
charOrNothing SQLNull = Nothing
charOrNothing (SQLChar a) = Just a

stringOrNothing :: SQLValue -> (Maybe String)
stringOrNothing SQLNull = Nothing
stringOrNothing (SQLString a) = Just a

fromStringOrNull :: SQLValue -> String
fromStringOrNull SQLNull = ""
fromStringOrNull (SQLString a) = a

boolOrNothing :: SQLValue -> (Maybe Bool)
boolOrNothing SQLNull = Nothing
boolOrNothing (SQLBool a) = Just a

dateOrNothing :: SQLValue -> (Maybe ClockTime)
dateOrNothing SQLNull = Nothing
dateOrNothing (SQLDate a) = Just a