module Pure.Bloom.Scalable 
  ( Bloom(..), 
    fromBloom,
    new, 
    add, 
    test, 
    update,
    size, 
    encode,
    decode,
  ) where

import qualified Pure.Bloom as Bloom

import Pure.Data.Txt as Txt (Txt,ToTxt(..),foldl')
import Pure.Data.JSON hiding ((!),encode,decode)

import Control.Monad.IO.Class
import Data.Array.MArray
import Data.Array.IO
import Data.Array.Unboxed
import Data.Array.Unsafe

import Control.Concurrent.MVar
import Control.Monad (foldM,unless,when,join,void)
import Data.Bits
import Data.Char
import Data.Foldable
import Data.Function
import Data.IORef
import Data.List as List
import Data.Maybe
import Data.Traversable
import Data.Word

data Bloom = Bloom
  { epsilon :: {-# UNPACK #-}!Double
  , hashes  :: {-# UNPACK #-}!Int 
  , buckets :: {-# UNPACK #-}!Int 
  , factor  :: {-# UNPACK #-}!Double
  , quota   :: {-# UNPACK #-}!(IORef (Int,Int))
  , blooms  :: {-# UNPACK #-}!(IORef [Bloom.Bloom])
  }

fromBloom :: MonadIO m => Bloom.Bloom -> m Bloom
fromBloom b = fromBloomWith b 2

fromBloomWith :: MonadIO m => Bloom.Bloom -> Double -> m Bloom
fromBloomWith b@Bloom.Bloom {..} factor = liftIO $ do
  sz     <- Bloom.size b
  let mx = Bloom.maximumSize b
  quota  <- newIORef (mx,sz)
  blooms <- newIORef [b]
  pure Bloom {..}


encode :: MonadIO m => Bloom -> m Value
encode Bloom {..} = liftIO $ do
  (s,c) <- readIORef quota
  bs <- readIORef blooms >>= traverse Bloom.encode
  pure $
    object
      [ "epsilon"  .= epsilon
      , "hashes"   .= hashes 
      , "buckets"  .= buckets
      , "factor"   .= factor
      , "quota"    .= (s,c)
      , "blooms"   .= bs 
      ]

decode :: MonadIO m => Value -> m (Maybe Bloom)
decode v
  | Just (epsilon,hashes,buckets,factor,q,bs) <- fields = liftIO $ do 
    traverse Bloom.decode bs >>= \blooms ->
      if all isJust blooms then do
        quota  <- newIORef q
        blooms <- newIORef (catMaybes blooms)
        pure (Just Bloom {..})
      else
        pure Nothing
  | otherwise = 
    pure Nothing
  where
    fields :: Maybe (Double,Int,Int,Double,(Int,Int),[Value])
    fields = parse v $ withObject "Bloom" $ \o -> do
      epsilon <- o .: "epsilon"
      hashes  <- o .: "hashes"
      buckets <- o .: "buckets"
      factor  <- o .: "factor" 
      quota   <- o .: "quota"
      blooms  <- o .: "blooms"
      pure (epsilon,hashes,buckets,factor,quota,blooms)

{-# INLINE bloom #-}
bloom :: Double -> Int -> IO Bloom
bloom = new

{-# INLINE new #-}
new :: Double -> Int -> IO Bloom
new epsilon size = newWith epsilon size 2

{-# INLINE newWith #-}
newWith :: MonadIO m => Double -> Int -> Double -> m Bloom
newWith epsilon size factor = liftIO $ do
  b@(Bloom.Bloom _ hashes buckets _ _) <- Bloom.new epsilon size
  quota <- newIORef (size,0)
  blooms <- newIORef [b]
  pure Bloom {..}

{-# INLINE add #-}
add :: (MonadIO m, ToTxt a) => Bloom -> a -> m ()
add bs a = void (update bs a)

{-# INLINE update #-}
update :: (MonadIO m, ToTxt a) => Bloom -> a -> m Bool
update bs@Bloom {..} val = liftIO $ do
  b <- test bs val
  unless b $ do
    bs <- readIORef blooms
    added <- Bloom.update (head bs) val
    let 
      grow n = do
        b <- Bloom.new epsilon n
        atomicModifyIORef' blooms $ \bs -> (b:bs,())

    when added do
      join $ atomicModifyIORef' quota $ \(total,current) ->
        if current + 1 == total then
          let total' = round (fromIntegral total * factor)
          in ((total',current + 1),grow total')
        else
          ((total,current + 1),pure ())
   
  pure (not b)

{-# INLINE test #-}
test :: (MonadIO m, ToTxt a) => Bloom -> a -> m Bool
test Bloom {..} (toTxt -> val) = liftIO $ do
  bs <- readIORef blooms
  let hs = hash (head bs) val
  or <$> traverse (go hs) bs
  where
    go :: [Int] -> Bloom.Bloom -> IO Bool
    go hs Bloom.Bloom { hashes, buckets, bits } = 
      and <$> traverse (readArray bits . (`mod` buckets)) (List.take hashes hs)

{-# INLINE size #-}
size :: MonadIO m => Bloom -> m Int
size Bloom { quota } = liftIO $ do
  snd <$> readIORef quota

-- FNV-1a 
{-# INLINE fnv64 #-}
fnv64 :: Txt -> Word64
fnv64 = Txt.foldl' h 0xcbf29ce484222325
  where
    {-# INLINE h #-}
    h :: Word64 -> Char -> Word64
    h !i c = 
      let i' = i `xor` fromIntegral (ord c) 
      in i' * 0x100000001b3

{-# INLINE hash #-}
hash :: Bloom.Bloom -> Txt -> [Int]
hash (Bloom.Bloom _ hashes buckets _ _) val =
  let
    -- We want the list of hashes to be lazy, but
    -- we know that at least 1 will always be materialized,
    -- so go ahead and make these constants strict.
    !h = fnv64 val
    !hi = fromIntegral (shiftR h 32)
    !lo = fromIntegral (h .&. 0x00000000FFFFFFFF)
  in
    fmap (\i -> hi + lo * i) [1..]



