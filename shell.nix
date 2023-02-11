{ pkgs ? import <nixpkgs> {}
}:
pkgs.mkShell {
	name = "lucky-pool";
	buildInputs = [
		pkgs.docker-compose
		pkgs.inetutils
	];
	shellHook = ''
		echo 'hoge'
	'';
}
