package validators

import "regexp"

func ValidateTopicName(name string) []string {
	var res []string
	if len(name) <= 0 || len(name) > 255 {
		res = append(res, "Topic name must be between 0 and 255 characters")
	}
	if match, _ := regexp.MatchString("\\A[A-Za-z0-9\\.\\-_]+\\z", name); !match {
		res = append(res, "Topic name can only contain letters, numbers and dot, underline and strike (., _, -).")
	}
	return res
}
