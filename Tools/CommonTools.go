package Tools

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"regexp"
)

func GetHash(inStr string) string {
	UnifyString := toUnify(inStr)
	Sum := md5.Sum([]byte(UnifyString))
	return fmt.Sprintf("%x", Sum)
}

func toUnify(inStr string) string {
	re := regexp.MustCompile(`[\n\s'\.;:\(\)\"\'\d]`)
	return re.ReplaceAllString(inStr, "")
}

func Uuid() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("Error: ", err)
		return ""
	} else {
		return fmt.Sprintf("%X", b)
	}
}
