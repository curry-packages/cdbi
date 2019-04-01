------------------------------------------------------------------------------
--- Library for handling global entities.
--- A global entity has a name declared in the program.
--- Its value can be accessed and modified by IO actions.
--- Furthermore, global entities can be declared as persistent so that
--- their values are stored across different program executions.
---
--- Currently, it is still experimental so that its interface might
--- be slightly changed in the future.
---
--- A global entity `g` with an initial value `v`
--- of type `t` must be declared by:
---
---     g :: Global t
---     g = global v spec
---
--- Here, the type `t` must not contain type variables and
--- `spec` specifies the storage mechanism for the
--- global entity (see type `GlobalSpec`).
---
---
--- @author Michael Hanus
--- @version February 2017
--- @category general
------------------------------------------------------------------------------
{-# LANGUAGE CPP #-}

module Global( Global, GlobalSpec(..), global
             , readGlobal, safeReadGlobal, writeGlobal)
 where

----------------------------------------------------------------------

--- The abstract type of a global entity.
#ifdef __PAKCS__
data Global a = GlobalDef a GlobalSpec
#else
external data Global _
#endif

--- `global` is only used for the declaration of a global value
--- and should not be used elsewhere. In the future, it might become a keyword.
global :: a -> GlobalSpec -> Global a
#ifdef __PAKCS__
global v s = GlobalDef v s
#else
global external
#endif

--- The storage mechanism for the global entity.
--- @cons Temporary - the global value exists only during a single execution
---                   of a program
--- @cons Persistent f - the global value is stored persisently in file f
---                     (which is created and initialized if it does not exists)
data GlobalSpec = Temporary  | Persistent String


--- Reads the current value of a global.
readGlobal :: Global a -> IO a
readGlobal g = prim_readGlobal $# g

prim_readGlobal :: Global a -> IO a
prim_readGlobal external

--- Safely reads the current value of a global.
--- If `readGlobal` fails (e.g., due to a corrupted persistent storage),
--- the global is re-initialized with the default value given as
--- the second argument.
safeReadGlobal :: Global a -> a -> IO a
safeReadGlobal g dflt =
  catch (readGlobal g) (\_ -> writeGlobal g dflt >> return dflt)

--- Updates the value of a global.
--- The value is evaluated to a ground constructor term before it is updated.
writeGlobal :: Global a -> a -> IO ()
writeGlobal g v = (prim_writeGlobal $# g) $## v

prim_writeGlobal :: Global a -> a -> IO ()
prim_writeGlobal external


------------------------------------------------------------------------
