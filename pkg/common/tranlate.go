package common

import (
	"encoding/json"
	"fmt"
	"os"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type Translate struct {
	util Util
	lg   string
}

func NewTranslate(util Util, lg string) *Translate {
	return &Translate{
		util: util,
		lg:   lg,
	}
}
func (t *Translate) SetLanguage(lg string) {
	t.lg = lg
}

// Load translations from JSON file and register them
func (t Translate) LoadTranslations() error {
	var lgCodes = []string{"fr", "en"}
	var tags = map[string]language.Tag{
		"fr": language.French,
		"en": language.English,
	}
	for _, lg := range lgCodes {

		filename := fmt.Sprintf("%v/i18n/%v.json", *t.util.rootDirPath, lg)
		data, err := os.ReadFile(filename)
		if err != nil {
			return err
		}
		// log.Println(string(data))

		var translations map[string]string
		if err := json.Unmarshal(data, &translations); err != nil {
			return err
		}

		for key, value := range translations {
			message.SetString(tags[lg], key, value)
		}
	}
	return nil
}

func (t Translate) T(word string) string {

	var tag language.Tag

	if t.lg == "fr" {
		tag = language.French
	}
	if t.lg == "en" {
		tag = language.English
	}

	p := message.NewPrinter(tag)
	return p.Sprintf(word)
}
