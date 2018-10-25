# GoParsLog_1C
Утилита для парса логов технологического журнала 1С

Новые шаблоны регулярных выражений добавляются в метод **BuildChain()** пакета Tools
```go
func BuildChain() *Chain {
	Element1 := Chain{
		regexp:         regexp.MustCompile(`(?si)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Module=(?P<Module>[^,]+)(?:.+?)Method=(?P<Method>[^,]+)(?:.+?)MemoryPeak=(?P<Value>[\d]+)`),
		AgregateFileld: []string{"event", "DB", "Module", "Method"},
		OutPattern:     "(%DB%) CALL, количество - %count%, MemoryPeak - %Value%\n%Module%.%Method%",
	}

	Element2 := Chain{
		regexp:         regexp.MustCompile(`(?si)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Context=(?P<Context>[^,]+)(?:.+?)MemoryPeak=(?P<Value>[\d]+)`),
		NextElement:    &Element1,
		AgregateFileld: []string{"DB", "Context"},
		OutPattern:     "(%DB%) CALL, количество - %count%, MemoryPeak - %Value%\n%Context%",
	}

	Element3 := Chain{
		regexp:         regexp.MustCompile(`(?si)[,]EXCP,(?:.*?)process=(?P<Process>[^,]+)(?:.*?)Descr=(?P<Context>[^,]+)`),
		NextElement:    &Element2,
		AgregateFileld: []string{"Process", "Context"},
		OutPattern:     "(%Process%) EXCP, количество - %count%\n%Context%",
	}
	return &Element3
}
```
   
BuildChain() должен возврящать последнее "звено" цепочки.
- regexp -шаблон регулярного выражения. Группы захвата обязательно должны быть именованными, в Go это делается так (?P<Имя> .....)
- NextElement - ссылка на предыдущее звено цепочки
- AgregateFileld - имена групп захвата по которым будет производиться агрегация 
- OutPattern - шаблон по которому будет выводиться результат. В примере выше маркер %count% нигде не задается, это количество подходящих элементов в группе (при агрегации), давайте считать этот маркер "системным". Группа захвата содержащие значение которое будет суммироваться должна называться Value (имя групп регистрозависимое). Например, если мы захотим агрегировать значения duration, тогда регулярка будет такой `(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)........` -


Работать с цепочкой в основном окде нужно через sync.Pool **ChainPool**

