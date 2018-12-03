package Tools

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
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

var ChainPool = sync.Pool{
	New: func() interface{} {
		return BuildChain()
	},
}

func BuildChain() *Chain {
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

	Element2 := Chain{
		//preCondition:   func(In string) bool { return strings.Contains(In, ",CALL") },
		regexp:         regexp.MustCompile(`(?si)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Module=(?P<Module>[^,]+)(?:.+?)Method=(?P<Method>[^,]+)(?:.+?)MemoryPeak=(?P<Value>[\d]+)`),
		NextElement:    &Element1,
		AgregateFileld: []string{"event", "DB", "Module", "Method"},
		OutPattern:     "(%DB%) CALL, количество - %count%, MemoryPeak - %Value%\n%Module%.%Method%",
	}

	Element3 := Chain{
		regexp:         regexp.MustCompile(`(?si)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Context=(?P<Context>[^,]+)(?:.+?)MemoryPeak=(?P<Value>[\d]+)`),
		NextElement:    &Element2,
		AgregateFileld: []string{"DB", "Context"},
		OutPattern:     "(%DB%) CALL, количество - %count%, MemoryPeak - %Value%\n%Context%",
	}

	Element4 := Chain{
		regexp:         regexp.MustCompile(`(?si)[,]EXCP,(?:.*?)process=(?P<Process>[^,]+)(?:.*?)Descr=(?P<Context>[^,]+)`),
		NextElement:    &Element3,
		AgregateFileld: []string{"Process", "Context"},
		OutPattern:     "(%Process%) EXCP, количество - %count%\n%Context%",
	}
	return &Element4
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
	for k, v := range result {
		for _, j := range c.AgregateFileld {
			if j == k {
				KeyStr += v
			}
		}

		// Value потом будет агригороваться
		if k != "" && k != "Value" {
			OutStr = strings.Replace(OutStr, "%"+k+"%", strings.Trim(v, " \n"), -1)
		}
	}

	v, _ := strconv.ParseInt(result["Value"], 10, 64)
	return GetHash(KeyStr), OutStr, v
}
