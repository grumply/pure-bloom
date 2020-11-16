{ mkDerivation, array, base, pure-json, pure-txt, stdenv }:
mkDerivation {
  pname = "pure-bloom";
  version = "0.8.0.0";
  src = ./.;
  libraryHaskellDepends = [ array base pure-json pure-txt ];
  homepage = "github.com/grumply/pure-bloom";
  license = stdenv.lib.licenses.bsd3;
}
