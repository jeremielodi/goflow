package common

import (
	"encoding/base64"
	"fmt"
	"os"
	"path"
)

type Images struct {
	util Util
}

func NewImages(util Util) *Images {
	return &Images{
		util: util,
	}
}
func (img *Images) SaveBase64File(base64Image string) (bool, string, error) {
	// Strip the data URL prefix if present
	const prefix = "data:image/png;base64,"
	if len(base64Image) > len(prefix) && base64Image[:len(prefix)] == prefix {
		base64Image = base64Image[len(prefix):]
	}

	// Decode the Base64 string
	imageData, err := base64.StdEncoding.DecodeString(base64Image)
	if err != nil {
		fmt.Println("Error decoding Base64 string:", err)
		return false, "", err
	}

	// Write the image data to a file
	imageName := fmt.Sprintf("%s.png", img.util.Uuid()) // Specify the output file path
	filePath := path.Join(*img.util.rootDirPath, "..", "server", "upload", imageName)
	err = os.WriteFile(filePath, imageData, 0644)
	if err != nil {
		fmt.Println("Error writing file:", err)
		return false, "", err
	}
	return true, imageName, nil
}
