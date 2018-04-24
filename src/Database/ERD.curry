------------------------------------------------------------------------------
--- This module contains the definition of data types to represent
--- entity/relationship diagrams and an I/O operation to read them
--- from a term file.
---
--- @author Michael Hanus, Marion Mueller
--- @version April 2018
--- @category database
------------------------------------------------------------------------------

module Database.ERD
  ( ERD(..), ERDName, Entity(..), EName, Entity(..)
  , Attribute(..), AName, Key(..), Null, Domain(..)
  , Relationship(..), REnd(..), RName, Role, Cardinality(..), MaxValue(..)
  , readERDTermFile, writeERDTermFile
  ) where

import Char         (isSpace)
import Directory    (getAbsolutePath)
import IO
import ReadShowTerm (readUnqualifiedTerm)
import Time

--- Data type to represent entity/relationship diagrams.
--- The components are the name of the ER model, the list of entities,
--- and the list of relationships.
data ERD = ERD ERDName [Entity] [Relationship]
  deriving Show

--- The name of an ER model (a string).
type ERDName = String -- used as the name of the generated module

--- Data type to represent the entities of an ER model.
--- Each entity consists of a name and a list of attributes.
data Entity = Entity EName [Attribute]
  deriving Show

--- The name of an entity (a string).
type EName = String

--- Data type to represent attributes of entities of an ER model.
--- Each attribute consists of
--- * a name
--- * the domain (i.e., type) of the attribute
--- * a value specifying the key property of thi attribute
---   (no key, primary key, or unique)
--- * a flag indicating whether this attribute can contain null values
data Attribute = Attribute AName Domain Key Null
  deriving Show

--- The name of an attribute (a string).
type AName = String

--- Data type to represent key properties of attributes
--- (no key, primary key, or unique).
data Key = NoKey
         | PKey
         | Unique
  deriving (Eq, Show)

--- Type of the flag of an attribute indicating whether the attribute
--- can contain null values (if the flag has value `True`).
type Null = Bool

--- Data type the domain of an attribute.
--- If the attribute has a default value, it can be specified
--- as an argument in the domain.
data Domain = IntDom      (Maybe Int)
            | FloatDom    (Maybe Float)
            | CharDom     (Maybe Char)
            | StringDom   (Maybe String)
            | BoolDom     (Maybe Bool)
            | DateDom     (Maybe CalendarTime)
            | UserDefined String (Maybe String)
            | KeyDom      String  -- for foreign keys
  deriving Show


--- Data type to represent the relationships of an ER model.
--- Each relationship consists of a name and a list of end points
--- (usually with two elements).
data Relationship = Relationship RName [REnd]
  deriving Show

--- The name of a relationship (a string).
type RName = String

--- An end point of a relationship which consists of the name
--- of an entity, the name of the role, and a cardinality constraint.
data REnd = REnd EName Role Cardinality
  deriving Show

--- The name of a role (a string).
type Role = String

--- Cardinality of a relationship w.r.t. some entity.
--- The cardinality is either a fixed number (e.g., (Exactly 1)
--- representing the cardinality (1,1))
--- or an interval (e.g., (Between 1 (Max 4)) representing the
--- cardinality (1,4), or (Between 0 Infinite) representing the
--- cardinality (0,n)).
data Cardinality = Exactly Int
                 | Between Int MaxValue
  deriving Show

--- The upper bound of a cardinality which is either a finite number
--- or infinite.
data MaxValue = Max Int | Infinite
  deriving Show


--- Read an ERD specification from a file containing a single ERD term.
readERDTermFile :: String -> IO ERD
readERDTermFile termfilename = do
  putStrLn $ "Reading ERD term from file '" ++ termfilename ++ "'..."
  handle <- openFile termfilename ReadMode
  line <- skipCommentLines handle
  termstring <- hGetContents handle
  return (updateERDTerm (readUnqualifiedTerm ["Database.ERD","Prelude"]
                                             (unlines [line,termstring])))
 where
  skipCommentLines h = do
    line <- hGetLine h >>= return . dropWhile isSpace
    if null line || take 2 line == "--"
     then skipCommentLines h
     else if take 2 line == "{-" -- -}
          then skipBracketComment h (drop 2 line)
          else return line

  skipBracketComment h [] = hGetLine h >>= skipBracketComment h
  skipBracketComment h [_] = hGetLine h >>= skipBracketComment h
  skipBracketComment h (c1:c2:cs) =
   if c1=='-' && c2=='}' then return cs
                         else skipBracketComment h (c2:cs)

--- Transforms an ERD term possible containing old, outdated, information.
--- In particular, translate (Range ...) into (Between ...).
updateERDTerm :: ERD -> ERD
updateERDTerm (ERD name es rs) = ERD name es (map updateRel rs)
 where
   updateRel (Relationship r ends) = Relationship r (map updateEnd ends)

   updateEnd (REnd n r c) = REnd n r (updateCard c)

   updateCard (Exactly n) = Exactly n
   updateCard (Between min (Max m)) =
     if min<=m
     then Between min (Max m)
     else error ("ERD: Illegal cardinality " ++ show (Between min (Max m)))
   updateCard (Between min Infinite) = Between min Infinite

--- Writes an ERD term into a file with name `ERDMODELNAME.erdterm`
--- and returns the absolute path name of the generated term file.
writeERDTermFile :: ERD -> IO String
writeERDTermFile erd@(ERD name _ _) = do
  let termfile = name ++ ".erdterm"
  writeFile termfile (show erd)
  getAbsolutePath termfile

{-
-- Example ERD term:
(ERD "Uni"
 [Entity "Student" [Attribute "MatNum"    (IntDom Nothing) PKey False,
                    Attribute "Name"      (StringDom Nothing) NoKey False,
                    Attribute "Firstname" (StringDom Nothing) NoKey False,
                    Attribute "Email"     (UserDefined "MyModule.Email" Nothing)
                                          NoKey True],
  Entity "Lecture" [Attribute "Id"    (IntDom Nothing) PKey False,
                    Attribute "Title" (StringDom Nothing) Unique False,
                    Attribute "Hours" (IntDom (Just 4)) NoKey False],
  Entity "Lecturer" [Attribute "Id"        (IntDom Nothing) PKey False,
                     Attribute "Name"      (StringDom Nothing) NoKey False,
                     Attribute "Firstname" (StringDom Nothing) NoKey False],
  Entity "Group" [Attribute "Time" (StringDom Nothing) NoKey False]]
 [Relationship "Teaching"
               [REnd "Lecturer" "taught_by" (Exactly 1),
                REnd "Lecture"  "teaches"   (Between 0 Infinite)],
  Relationship "Participation"
               [REnd "Student" "participated_by" (Between 0 Infinite),
                REnd "Lecture" "participates"    (Between 0 Infinite)],
  Relationship "Membership"
               [REnd "Student" "consists_of" (Exactly 3),
                REnd "Group" "member_of"     (Between 0 Infinite)]])

-}
