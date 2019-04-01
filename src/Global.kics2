import CurryException
import Control.Exception as C
import Data.IORef
import System.IO
import System.Directory  (doesFileExist)
import System.IO.Unsafe
import System.Process    (system)

-- Implementation of Globals in Curry. We use Haskell's IORefs for temporary
-- globals where Curry values are stored in the IORefs
data C_Global a
     = Choice_C_Global Cover ID (C_Global a) (C_Global a)
     | Choices_C_Global Cover ID ([C_Global a])
     | Fail_C_Global Cover FailInfo
     | Guard_C_Global Cover Constraints (C_Global a)
     | C_Global_Temp (IORef a)  -- a temporary global
     | C_Global_Pers String     -- a persistent global with a given (file) name

instance Show (C_Global a) where
  show = error "ERROR: no show for Global"

instance Read (C_Global a) where
  readsPrec = error "ERROR: no read for Global"

instance NonDet (C_Global a) where
  choiceCons = Choice_C_Global
  choicesCons = Choices_C_Global
  failCons = Fail_C_Global
  guardCons = Guard_C_Global
  try (Choice_C_Global cd i x y) = tryChoice cd i x y
  try (Choices_C_Global cd i xs) = tryChoices cd i xs
  try (Fail_C_Global cd info) = Fail cd info
  try (Guard_C_Global cd c e) = Guard cd c e
  try x = Val x
  match choiceF _ _ _ _ _ (Choice_C_Global cd i x y) = choiceF cd i x y
  match _ narrF _ _ _ _   (Choices_C_Global cd i@(NarrowedID _ _) xs)
   = narrF cd i xs
  match _ _ freeF _ _ _   (Choices_C_Global cd i@(FreeID _ _) xs)
   = freeF cd i xs
  match _ _ _ failF _ _   (Fail_C_Global cd info) = failF cd info
  match _ _ _ _ guardF _  (Guard_C_Global cd c e) = guardF cd c e
  match _ _ _ _ _ valF    x                    = valF x

instance Generable (C_Global a) where
  generate _ _ = error "ERROR: no generator for Global"

instance NormalForm (C_Global a) where
  ($!!) cont g@(C_Global_Temp _)         cd cs = cont g cd cs
  ($!!) cont g@(C_Global_Pers _)         cd cs = cont g cd cs
  ($!!) cont (Choice_C_Global d i g1 g2) cd cs = nfChoice cont d i g1 g2 cd cs
  ($!!) cont (Choices_C_Global d i gs)   cd cs = nfChoices cont d i gs cd cs
  ($!!) cont (Guard_C_Global d c g)      cd cs = guardCons d c ((cont $!! g) cd
                                                    $! (addCs c cs))
  ($!!) _    (Fail_C_Global d info)      _  _  = failCons d info
  ($##) cont g@(C_Global_Temp _)         cd cs = cont g cd cs
  ($##) cont g@(C_Global_Pers _)         cd cs = cont g cd cs
  ($##) cont (Choice_C_Global d i g1 g2) cd cs = gnfChoice cont d i g1 g2 cd cs
  ($##) cont (Choices_C_Global d i gs)   cd cs = gnfChoices cont d i gs cd cs
  ($##) cont (Guard_C_Global d c g)      cd cs = guardCons d c ((cont $## g) cd
                                                    $!  (addCs c cs))
  ($##) _    (Fail_C_Global cd info)     _  _  = failCons cd info
  searchNF _ cont g@(C_Global_Temp _)          = cont g
  searchNF _ cont g@(C_Global_Pers _)          = cont g

instance Unifiable (C_Global a) where
  (=.=) (C_Global_Temp ref1) (C_Global_Temp ref2) _ _
    | ref1 == ref2 = C_True
  (=.=) (C_Global_Pers f1) (C_Global_Pers f2) _ _
    | f1 == f2  = C_True
  (=.=) _ _ cd _ = Fail_C_Bool cd defFailInfo
  (=.<=) = (=.=)
  bind cd i (Choice_C_Global d j l r)
    = [(ConstraintChoice d j (bind cd i l) (bind cd i r))]
  bind cd i (Choices_C_Global d j@(FreeID _ _) xs) = bindOrNarrow cd i d j xs
  bind cd i (Choices_C_Global d j@(NarrowedID _ _) xs)
    = [(ConstraintChoices d j (map (bind cd i) xs))]
  bind _ _ (Fail_C_Global _ info) = [Unsolvable info]
  bind cd i (Guard_C_Global _ cs e) = (getConstrList cs) ++ (bind cd i e)
  lazyBind cd i (Choice_C_Global d j l r)
    = [(ConstraintChoice d j (lazyBind cd i l) (lazyBind cd i r))]
  lazyBind cd i (Choices_C_Global d j@(FreeID _ _) xs) = lazyBindOrNarrow cd i d j xs
  lazyBind cd i (Choices_C_Global d j@(NarrowedID _ _) xs)
    = [(ConstraintChoices d j (map (lazyBind cd i) xs))]
  lazyBind _ _ (Fail_C_Global cd info) = [Unsolvable info]
  lazyBind cd i (Guard_C_Global _ cs e)
    = (getConstrList cs) ++ [(i :=: (LazyBind (lazyBind cd i e)))]

instance Curry_Prelude.Curry a => Curry_Prelude.Curry (C_Global a)


external_d_C_global :: Curry_Prelude.Curry a => a -> C_GlobalSpec -> Cover -> ConstStore
                    -> C_Global a
external_d_C_global val C_Temporary _ _ = ref `seq` (C_Global_Temp ref)
  where ref = unsafePerformIO (newIORef val)
external_d_C_global val (C_Persistent cname) _ _ =
  let name = fromCurry cname :: String
   in unsafePerformIO (initGlobalFile name >> return (C_Global_Pers name))
 where initGlobalFile name = do
         ex <- doesFileExist name
         if ex then return ()
               else writeFile name (show val++"\n")

external_d_C_prim_readGlobal :: Curry_Prelude.Curry a => C_Global a -> Cover -> ConstStore
                             -> Curry_Prelude.C_IO a
external_d_C_prim_readGlobal (C_Global_Temp  ref) _ _ = fromIO (readIORef ref)
external_d_C_prim_readGlobal (C_Global_Pers name) _ _ = fromIO $
  exclusiveOnFile name $ do
    s <- catch (do h <- openFile name ReadMode
                   eof <- hIsEOF h
                   s <- if eof then return "" else hGetLine h
                   hClose h
                   return s)
              (\e -> throw (IOException (show (e :: C.IOException))))
    case reads s of
      [(val,"")] -> return val
      _          -> throw (IOException $ "Persistent file `" ++ name ++
                                         "' contains malformed contents")

external_d_C_prim_writeGlobal :: Curry_Prelude.Curry a => C_Global a -> a
                              -> Cover -> ConstStore -> Curry_Prelude.C_IO Curry_Prelude.OP_Unit
external_d_C_prim_writeGlobal (C_Global_Temp ref) val _ _ =
  toCurry (writeIORef ref val)
external_d_C_prim_writeGlobal (C_Global_Pers name) val _ _ =
  toCurry (exclusiveOnFile name $ writeFile name (show val ++ "\n"))


--- Forces the exclusive execution of an action via a lock file.
exclusiveOnFile :: String -> IO a -> IO a
exclusiveOnFile file action = do
  exlock <- doesFileExist lockfile
  if exlock
    then hPutStrLn stderr
              (">>> Waiting for removing lock file `" ++ lockfile ++ "'...")
    else return ()
  system ("lockfile-create --lock-name "++lockfile)
  C.catch (do actionResult <- action
              deleteLockFile
              return actionResult )
          (\e -> deleteLockFile >> C.throw (e :: CurryException))
 where
  lockfile = file ++ ".LOCK"
  deleteLockFile = system $ "lockfile-remove --lock-name " ++ lockfile
