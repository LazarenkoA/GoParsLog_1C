package Tools

import (
	"regexp"
	"strconv"
	"strings"
)

type IChain interface {
	Execute(SourceStr string) (string, string, int64)
}

type Chain struct {
	OutPattern     string
	NextElement    *Chain
	preCondition   func(in string) bool
	regexp         *regexp.Regexp
	AgregateFileld []string
}

type PatternsData struct {
	AgregateFileld []string
	OutPattern     string
	RegexpPattern  string
}

type PatternList struct {
	List []*PatternsData `xml:"PatternsData"`
}

/* var ChainPool = sync.Pool{
	New: func() interface{} {
		return BuildChain()
	},
} */

func BuildChain(P PatternList) *Chain {
	if len(P.List) == 0 {
		// цепочка по дефолту

		Element0 := Chain{
			regexp:         regexp.MustCompile(`(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Module=(?P<Module>[^,]+)(?:.+?)Method=(?P<Method>[^,]+)`),
			AgregateFileld: []string{"event", "DB", "Module", "Method"},
			OutPattern:     "(%DB%) CALL, количество - %count%, duration - %Value%\n%Module%.%Method%",
		}

		Element1 := Chain{
			regexp:         regexp.MustCompile(`(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,](?P<event>[^,]+)(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)(Context=(?P<Context>[^,]+))`),
			NextElement:    &Element0,
			AgregateFileld: []string{"event", "DB", "Context"},
			OutPattern:     "(%DB%) %event%, количество - %count%, duration - %Value%\n%Context%",
		}

		return &Element1
	} else {
		var lastElement *Chain
		for id, elem := range P.List {
			cohesion := func(currentElem *Chain) {
				if id != 0 {
					currentElem.NextElement = lastElement
				}
				lastElement = currentElem
			}
			Element := Chain{
				regexp:         regexp.MustCompile(elem.RegexpPattern),
				AgregateFileld: elem.AgregateFileld,
				OutPattern:     elem.OutPattern,
			}
			cohesion(&Element)
		}

		return lastElement
	}
}

func (c *Chain) Execute(SourceStr string) (string, string, int64) {
	exRegExp := func() map[string]string {
		matches := c.regexp.FindStringSubmatch(SourceStr)
		if len(matches) == 0 {
			return nil
		}

		// GroupsName - для работы с именоваными группами захвата.
		GroupsName := make(map[string]string)
		for id, name := range c.regexp.SubexpNames() {
			GroupsName[name] = matches[id]
		}

		return GroupsName
	}

	result := exRegExp()
	if result == nil {
		if c.NextElement != nil {
			return c.NextElement.Execute(SourceStr)
		}
		return "", "", 0
	}

	var KeyStr string
	var OutStr string = c.OutPattern

	for _, Fileld := range c.AgregateFileld {
		if v, ok := result[Fileld]; ok {
			KeyStr += v
			OutStr = strings.Replace(OutStr, "%"+Fileld+"%", strings.Trim(v, " \n\r "), -1)
		}
	}

	value, _ := strconv.ParseInt(result["Value"], 10, 64)
	return GetHash(KeyStr), OutStr, value
}
