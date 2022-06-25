package utils

// func loadConfig(path string) (map[string]interface{}, error) {

// 	confFile, err := os.Open(path)
// 	defer confFile.Close()

// 	loadConfigLogger = rootLogger.WithFields(log.Fields{"config_path": path})
// 	if err != nil {
// 		loadConfigLogger.Error(err)
// 		return nil, err
// 	} else {
// 		loadConfigLogger.Info("Configuration file found.")
// 	}

// 	jd := json.NewDecoder(confFile)
// 	err = jp.Decode(&confFile)

// 	if err != nil {
// 		loadConfigLogger.Panic(fmt.Sprintf("Failed to parse testconf: %s", err))
// 	}

// 	byteValue, _ := ioutil.ReadAll(jsonFile)
// 	// kafka.ConfigMap behaves as map[string]interface{}, 
// 	// so we can unmarshal unstructured json directly into it
// 	var consumerConfig map[string]interface{} = make(map[string]interface{})
// 	json.Unmarshal([]byte(byteValue), consumerConfig)
// 	rootLogger.WithFields(log.Fields{"config_path": path}).Infof("%v", consumerConfig)

// 	return consumerConfig, nil
// }
