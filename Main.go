package main

import (
	"bufio"
	"crypto/md5"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Data struct {
	value  int64
	count  int
	OutStr string
}

type IChain interface {
	Execute(SourceStr string)
}

type Chain struct {
	OutPattern     string
	NextElement    *Chain
	regexp         *regexp.Regexp
	AgregateFileld []string
}

var ChainPool = sync.Pool{
	New: func() interface{} {
		return BuildChain()
	},
}

//type mapData map[string]*Data
var FullData map[string]*Data
var Mutex = &sync.Mutex{}
var SortByCount, SortByValue, IO bool
var Top, Go int
var RootDir string
var DataBuffer [10]*Data

func main() {
	FullData = make(map[string]*Data)
	flag.BoolVar(&SortByCount, "SortByCount", false, "Сортировка по количеству вызовов (bool)")
	flag.BoolVar(&SortByValue, "SortByValue", false, "Сортировка по значению (bool)")
	flag.BoolVar(&IO, "io", false, "Флаг того, что данные будут поступать из StdIn (bool)")
	flag.IntVar(&Top, "Top", 100, "Ограничение на вывод по количеству записей")
	flag.IntVar(&Go, "Go", 10, "Количество воркеров которые будут обрабатывать файл")
	flag.StringVar(&RootDir, "RootDir", "", "Корневая директория")
	flag.Parse()

	if RootDir != "" {
		FindFiles(RootDir)
	} else if IO {
		readStdIn()
	} else {
		panic("Не определены входящие данные")
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
	return getHash(KeyStr), OutStr, v
}

//////////////////////////// Горутины ////////////////////////////////////////

func goReader(outChan <-chan map[string]*Data) {
	for input := range outChan {
		MergeData(input)
	}
}

func startWorker(inChan <-chan *string, outChan chan<- map[string]*Data, group *sync.WaitGroup) {
	defer group.Done()

	for input := range inChan {
		outChan <- ParsPart(input)
		runtime.Gosched() // Передаем управление другой горутине.
	}
}

func ParsFile(FilePath string, group *sync.WaitGroup) {
	defer group.Done()

	var file *os.File
	defer file.Close()
	file, er := os.Open(FilePath)

	if er != nil {
		fmt.Printf("Ошибка открытия файла %v\n\t%v", FilePath, er.Error())
		return
	}

	Scan := bufio.NewScanner(file)
	ParsStream(Scan)
}

func putInQueue(key string, value *Data) {

}

//////////////////////////////////////////////////////////////////////////////

func runWorkers(count int, inChan <-chan *string, outChan chan<- map[string]*Data, group *sync.WaitGroup) {
	for i := 0; i < count; i++ {
		group.Add(1)
		go startWorker(inChan, outChan, group)
	}
}

func BuildChain() *Chain {
	Element0 := Chain{
		regexp:         regexp.MustCompile(`(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,](?P<event>[^,]+)(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Module=(?P<Module>[^,]+)(?:.+?)Method=(?P<Method>[^,]+)`),
		AgregateFileld: []string{"event", "DB", "Module", "Method"},
		OutPattern:     "(%DB%) %event%, количество - %count%, duration - %Value%\n%Module%.%Method%",
	}

	Element1 := Chain{
		regexp:         regexp.MustCompile(`(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,](?P<event>[^,]+)(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)(Context=(?P<Context>[^,]+))`),
		NextElement:    &Element0,
		AgregateFileld: []string{"event", "DB", "Context"},
		OutPattern:     "(%DB%) %event%, количество - %count%, duration - %Value%\n%Context%",
	}

	/* 	Element2 := Chain{
		regexp:         regexp.MustCompile(`(?si)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Context=(?P<Context>[^,]+)(?:.+?)MemoryPeak=(?P<Value>[\d]+)`),
		NextElement:    &Element1,
		AgregateFileld: []string{"DB", "Context"},
		OutPattern:     "(%DB%) CALL, количество - %count%, MemoryPeak - %Value%\n%Context%",
	} */

	Element3 := Chain{
		regexp:         regexp.MustCompile(`(?si)[,]EXCP,(?:.*?)process=(?P<Process>[^,]+)(?:.*?)Descr=(?P<Context>[^,]+)`),
		NextElement:    &Element1,
		AgregateFileld: []string{"Process", "Context"},
		OutPattern:     "(%Process%) EXCP, количество - %count%\n%Context%",
	}
	return &Element3
}

func readStdIn() {
	in := bufio.NewScanner(os.Stdin)
	ParsStream(in)

	PrettyPrint()
}

func FindFiles(rootDir string) {

	start := time.Now()
	group := &sync.WaitGroup{}
	callBack := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Произошла ошибка при оиске файлов, каталог %q: %v\n", path, err)
			return err
		}
		if strings.HasSuffix(info.Name(), "log") {
			group.Add(1)
			go ParsFile(path, group)
		}
		return nil
	}

	filepath.Walk(rootDir, callBack) // Поиск файлов.
	group.Wait()

	PrettyPrint()

	elapsed := time.Now().Sub(start)
	fmt.Printf("Код выполнялся: %v\n", elapsed)
}

func MergeData(Data map[string]*Data) {
	for key, value := range Data {
		//go putInQueue(key, value)

		Mutex.Lock()
		if _, exist := FullData[key]; exist {
			FullData[key].value += value.value
			FullData[key].count += value.count
		} else {
			FullData[key] = value
		}
		Mutex.Unlock()
	}
}

func ParsStream(Scan *bufio.Scanner) {
	inChan := make(chan *string, Go)
	outChan := make(chan map[string]*Data, Go)
	localgroup := &sync.WaitGroup{} // Группа для ожидания выполнения пула воркеров.

	pattern := `(?mi)\d\d:\d\d\.\d+[-]\d+`
	re := regexp.MustCompile(pattern)

	runWorkers(Go, inChan, outChan, localgroup)
	for i := 0; i < Go; i++ {
		go goReader(outChan) // Отдельная горутина которая будет читать из канала.
	}

	buff := make([]string, 1)
	writeChan := func() {
		part := strings.Join(buff, "\n")
		inChan <- &part
		buff = nil // Очищаем
	}

	for ok := Scan.Scan(); ok == true; {
		txt := Scan.Text()
		if ok := re.MatchString(txt); ok {
			writeChan()
			buff = append(buff, txt)
		} else {
			// Если мы в этом блоке, значит у нас многострочное событие, накапливаем строки в буфер
			buff = append(buff, txt)
		}

		ok = Scan.Scan()
	}
	if len(buff) > 0 {
		writeChan()
	}

	close(inChan) // Закрываем канал на для чтения

	localgroup.Wait()

	close(outChan)
}

func PrettyPrint() {
	// переводим map в массив
	len := len(FullData)
	array := make([]*Data, len, len)
	i := 0
	for _, value := range FullData {
		array[i] = value
		i++
	}

	Top = int(math.Min(float64(Top), float64(len)))
	if SortByCount {
		SortCount := func(i, j int) bool { return array[i].count > array[j].count }
		sort.Slice(array, SortCount)
	} else if SortByValue {
		SortValue := func(i, j int) bool { return array[i].value > array[j].value }
		sort.Slice(array, SortValue)
	}
	for id := range array[:Top] {
		OutStr := array[id].OutStr
		OutStr = strings.Replace(OutStr, "%count%", fmt.Sprintf("%d", array[id].count), -1)
		OutStr = strings.Replace(OutStr, "%Value%", fmt.Sprintf("%d", array[id].value), -1)

		fmt.Println(OutStr + "\n")
	}
}

func ParsPart(Blob *string) map[string]*Data {
	Str := *Blob
	if Str == "" {
		return nil
	}

	RepChain := ChainPool.Get().(*Chain)
	key, data, value := RepChain.Execute(Str)
	result := Data{OutStr: data, value: value, count: 1}
	return map[string]*Data{getHash(key): &result}
}

func getHash(inStr string) string {
	UnifyString := ToUnify(inStr)
	Sum := md5.Sum([]byte(UnifyString))
	return fmt.Sprintf("%x", Sum)
}

func ToUnify(inStr string) string {
	re := regexp.MustCompile(`[\n\s'\.;:\(\)\"\'\d]`)
	return re.ReplaceAllString(inStr, "")
}
