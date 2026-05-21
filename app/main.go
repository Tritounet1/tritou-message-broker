package main

import (
	"flag"
	"tidy/src/api"
)

func main() {
	mode := flag.String("mode", "server", "server or client")
	flag.Parse()

	switch *mode {
	case "server":
		api.RunServer()
	case "client":
		// Le client est surtout là pour tester le server donc à voir si garder sinon dans tout les cas il sera converti en tests unitaire
		api.RunClient()
	}

}
