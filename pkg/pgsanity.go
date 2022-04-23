package pkg

import (
	"flag"
	"log"
	"os"
	"path/filepath"
)

func parse() string {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("pgsanity: missing valid file or directory argument")
	}
	if len(args) > 1 {
		log.Fatal("pgsanity: too many arguments, only 1 argument supported")
	}
	return args[0]
}

func Run() {
	input := parse()
	input, _ = filepath.Abs(input)
	fileInfo, err := os.Stat(input)
	if err != nil {
		log.Fatalf("pgsanity: file not found: %v", err)
	}
	if fileInfo.IsDir() {
		checkDir(input)
	} else {
		checkFile(input)
	}
}

func isSqlFile(f string) bool {
	return filepath.Ext(f) == ".sql"
}

func ensureSql(f string) {
	if !isSqlFile(f) {
		log.Fatalf("file %s does not have .sql extension in its filename.", f)
	}
}

func checkDir(dir string) {
	err := filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !isSqlFile(path) {
				return nil
			}
			checkFile(path)
			return nil
		})
	if err != nil {
		log.Fatalf("pgsanity: error while checking file: %v", err)
	}
}

func checkFile(sqlFile string) {
	ensureSql(sqlFile)
	log.Printf("checking %s", sqlFile)
	err := CheckSyntax(FromRawSQLFilePath(sqlFile))
	if err != nil {
		log.Fatalf("pgsanity: error checking syntax of file %s: %v", sqlFile, err)
	}
}
