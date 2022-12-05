package configfile

import (
	"os"
	"testing"

	ec "github.com/ppreeper/dbtools/pkg/errcheck"
)

func TestGetConf(t *testing.T) {
	t.Log("Logs are printed when a test fails")
	userConfigDir, err := os.UserConfigDir()
	ec.CheckErr(err)
	c := GetConf(userConfigDir + "/dbtools/config.yml")
	if err != nil {
		t.Error("error loading config file")

	}
	t.Log(c)
	// t.Fatal("Uncomment to fail tests")

}

// func TestGetDB(t *testing.T) {
// 	_, err := C.GetDB("postgresql_example")
// 	if err == nil {
// 		t.Log("postgresql_example database config found")
// 	}
// 	_, err = C.GetDB("not_exist")
// 	if err != nil {
// 		t.Log(fmt.Errorf("non_exist database config not found: %w", err))
// 	}
// }
