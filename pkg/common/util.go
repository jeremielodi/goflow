package common

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type Util struct {
	rootDirPath *string
}

func NewUtil(rootDirPath *string) *Util {
	return &Util{
		rootDirPath: rootDirPath,
	}
}

func (u Util) RoundDecimal(value float64, precision int) float64 {
	factor := math.Pow(10, float64(precision))
	return math.Round(value*factor) / factor
}
func (u Util) Fallback(values ...interface{}) any {
	var result any
	for _, value := range values {
		if value != nil && value != "" && value != 0 {
			result = value
			break
		}
	}
	return result
}

func (u Util) ToCSV(obj map[string]interface{}, showHeader bool) string {
	var headers []string
	var values []string

	for key, value := range obj {
		headers = append(headers, key)
		values = append(values, fmt.Sprintf("%v", value))
	}

	headerLine := strings.Join(headers, ", ") + "\n"
	valueLine := strings.Join(values, ", ") + "\n"

	if showHeader {
		return headerLine + valueLine
	}
	return valueLine
}

func (u Util) Uuid() string {

	var uuidString string = strings.ReplaceAll(uuid.NewString(), "-", "")
	uuidString = strings.ToUpper(uuidString)
	return uuidString
}

func (u Util) Istrue(val interface{}) bool {
	showDetails := false

	if val == nil {
		return false
	}
	switch v := val.(type) {
	case string:
		if v == "yes" || v == "true" {
			showDetails = true
		}
	case bool:
		if v {
			showDetails = true
		}
	case int:
		if v == 1 {
			showDetails = true
		}
	}
	return showDetails
}

func (u Util) ParseQueries(strMap map[string]string) map[string]interface{} {
	interfaceMap := make(map[string]interface{})
	for key, value := range strMap {
		interfaceMap[key] = value // Assign string value to interface{}
	}
	return interfaceMap
}

func (u Util) FormatBoolean(val any) bool {
	str, isString := val.(string)
	if isString {
		return strings.ToLower(str) == "yes" || strings.ToLower(str) == "true"
	}
	b, _ := val.(bool)
	return b
}

func (u Util) FallbackInt(value int, fallback int) int {
	if value != 0 {
		return value
	}
	return fallback
}

func (u Util) FallbackString(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
func (u Util) ParseStruc(input interface{}) map[string]interface{} {
	options := make(map[string]interface{})

	val := reflect.ValueOf(input)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i)

		// Skip unexported fields
		if !value.CanInterface() {
			continue
		}

		fieldName := field.Tag.Get("json")
		if fieldName == "" || fieldName == "-" {
			continue
		}

		fieldValue := value.Interface()

		// Skip zero-value fields
		if reflect.DeepEqual(fieldValue, reflect.Zero(value.Type()).Interface()) {
			continue
		}

		// Trim strings

		if strVal, ok := fieldValue.(string); ok {
			options[fieldName] = strings.TrimSpace(strVal)
		} else {
			if strVal, ok := fieldValue.(*string); ok {
				if strVal != nil {
					options[fieldName] = strings.TrimSpace(*strVal)
				} else {
					options[fieldName] = ""
				}
			} else {
				options[fieldName] = fieldValue
			}
		}
	}

	return options
}
func (u Util) GeneratePDF(htmlContent string) ([]byte, error) {
	// Create a new chromedp context
	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	// Prepare a data URL with the HTML content
	dataURL := "data:text/html," + url.PathEscape(htmlContent)

	// Run chromedp tasks
	var pdfBytes []byte
	err := chromedp.Run(ctx,
		chromedp.Navigate(dataURL),
		chromedp.Sleep(1*time.Second), // let it render
		chromedp.ActionFunc(func(ctx context.Context) error {
			var err error
			pdfBytes, _, err = page.PrintToPDF().Do(ctx)
			return err
		}),
	)
	if err != nil {
		return nil, err
	}

	return pdfBytes, nil
}

func (u Util) IsSet(text *string) bool {
	if text == nil {
		return false
	}
	newText := strings.Trim(*text, " ")
	return len(newText) > 0
}

func (u Util) GetMD5Hash(text string) string {

	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (u Util) GetSHA1Hash(text string) string {
	hasher := sha1.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (u Util) DotEnvVariable(key string) string {

	envPath := path.Join(*u.rootDirPath, "..", ".env")
	err := godotenv.Load(envPath)
	if err != nil {

		envPath := path.Join(*u.rootDirPath, ".env")
		err2 := godotenv.Load(envPath)
		if err2 != nil {
			fmt.Println("Error loading .env file")
			return ""
		}
	}

	return os.Getenv(key)
}

// Helper to convert interface{} to string safely
func (u Util) ToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v) // assuming integer-like numbers
	case bool:
		return fmt.Sprintf("%t", v)
	case nil:
		return ""
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(bytes)
	}
}

func (u Util) GenerateRandomFiveDigitNumber() int {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate a random number between 10000 and 99999
	return rand.Intn(90000) + 10000
}

func (u Util) Validate(s interface{}) []map[string]string {
	var errs []map[string]string

	var customMessages = map[string]string{
		"required": "%s is required.",
		"email":    "%s must be a valid email address.",
		"min":      "%s must be at least %s%s.",
		"max":      "%s must be at most %s%s.",
	}

	val := reflect.ValueOf(s)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()

	validate := validator.New()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i)

		// Skip unexported fields
		if !value.CanInterface() {
			continue
		}

		// Get field name from json tag
		jsonTag := field.Tag.Get("json")
		fieldName := strings.Split(jsonTag, ",")[0]
		if fieldName == "" || fieldName == "-" {
			fieldName = field.Name
		}

		// Get validation tag
		validateTag := field.Tag.Get("validate")
		if validateTag == "" {
			continue
		}

		// Handle pointer values
		var valInterface interface{}
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				valInterface = nil
			} else {
				valInterface = value.Elem().Interface()
			}
		} else {
			valInterface = value.Interface()
		}

		// Run validator
		err := validate.Var(valInterface, validateTag)
		if err != nil {
			if verr, ok := err.(validator.ValidationErrors); ok {
				for _, e := range verr {
					field := fieldName
					tag := e.Tag()
					param := e.Param()

					// Default message
					message := e.Error()

					if msgTemplate, ok := customMessages[tag]; ok {
						var suffix string

						kind := e.Kind() // reflect.Kind
						if tag == "min" || tag == "max" {
							switch kind {
							case reflect.String:
								suffix = " characters"
							case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
								reflect.Float32, reflect.Float64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
								suffix = "" // numeric, no suffix
							default:
								suffix = ""
							}
						}

						message = fmt.Sprintf(msgTemplate, field, param, suffix)
					}

					errs = append(errs, map[string]string{
						"Field":   field,
						"Message": message,
					})
				}
			}
		}
	}

	return errs
}

func (u Util) AsString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func (u Util) AsBool(m map[string]any, key string) (bool, bool) {
	if m == nil {
		return false, false
	}
	v, ok := m[key]
	if !ok || v == nil {
		return false, false
	}
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		lt := strings.ToLower(t)
		return lt == "true" || lt == "1" || lt == "yes", true
	case float64:
		return t != 0, true
	default:
		return false, false
	}
}

func (u Util) CollectDynamicLabelKeys(items []map[string]any) []string {
	seen := map[string]struct{}{}
	for _, it := range items {
		for k := range it {
			if strings.HasPrefix(k, "label::") {
				seen[k] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
