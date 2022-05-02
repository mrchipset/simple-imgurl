package main

import (
	"log"

	"simple.imgurl/utils"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	utils.RunMinioService()
}
