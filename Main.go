package main

import (
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"encoding/gob"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
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

const AddSizeChan = 100

var SortByCount, SortByValue, IO bool
var Top, Go int
var RootDir string
var DataBuffer [5][]map[string]*Data

func main() {
	//FullData = make(map[string]*Data)
	flag.BoolVar(&SortByCount, "SortByCount", false, "Сортировка по количеству вызовов (bool)")
	flag.BoolVar(&SortByValue, "SortByValue", false, "Сортировка по значению (bool)")
	flag.BoolVar(&IO, "io", false, "Флаг того, что данные будут поступать из StdIn (bool)")
	flag.IntVar(&Top, "Top", 100, "Ограничение на вывод по количеству записей")
	flag.IntVar(&Go, "Go", 10, "Количество воркеров которые будут обрабатывать файл")
	flag.StringVar(&RootDir, "RootDir", "", "Корневая директория")
	flag.Parse()

	//FindFiles(`D:\1C_Log\Lazarenko\CALL_SCALL_BD`)
	//return

	if RootDir != "" {
		FindFiles(RootDir)
	} else if IO {
		readStdIn()
	} else {
		panic("Не определены входящие данные")
	}

}

//////////////////////////// Горутины ////////////////////////////////////////

func goMergeData(outChan <-chan map[string]*Data, resultChan chan<- map[string]*Data, G *sync.WaitGroup) {
	defer G.Done()

	var Data = make(map[string]*Data)
	for input := range outChan {
		MergeData(input, Data)
		//SerializationAndSave(input, TempDir)
		runtime.Gosched() // Передаем управление другой горутине.
	}
	resultChan <- Data
}

func goPrettyPrint(resultChan <-chan map[string]*Data, G *sync.WaitGroup) {
	defer G.Done()

	var resultData = make(map[string]*Data)
	for input := range resultChan {
		MergeData(input, resultData)
	}

	PrettyPrint(resultData)
}

func startWorker(inChan <-chan *string, outChan chan<- map[string]*Data, group *sync.WaitGroup) {
	defer group.Done()

	for input := range inChan {
		outChan <- ParsPart(input)
		runtime.Gosched() // Передаем управление другой горутине.
	}
}

func ParsFile(FilePath string, mergeChan chan<- map[string]*Data, group *sync.WaitGroup) {
	defer group.Done()

	var file *os.File
	defer file.Close()
	file, er := os.Open(FilePath)

	if er != nil {
		fmt.Printf("Ошибка открытия файла %q\n\t%v", FilePath, er.Error())
		return
	}

	ParsStream(bufio.NewScanner(file), mergeChan)
}

//////////////////////////////////////////////////////////////////////////////

///////////////////////// Сериализация ///////////////////////////////////////

func SerializationAndSave(inData map[string]*Data, TempDir string) {

	file, err2 := os.Create(path.Join(TempDir, uuid()))
	defer file.Close()
	if err2 != nil {
		fmt.Println("Ошибка создания файла:\n", err2.Error())
		return
	}

	Serialization(inData, file)
}

func Serialization(inData map[string]*Data, outFile *os.File) {
	Encode := gob.NewEncoder(outFile)

	err := Encode.Encode(inData)
	if err != nil {
		fmt.Println("Ошибка создания Encode:\n", err.Error())
		return
	}
}

func deSerialization(filePath string) (map[string]*Data, error) {
	var file *os.File
	file, err := os.Open(filePath)
	defer os.Remove(filePath)
	defer file.Close()

	if err != nil {
		fmt.Printf("Ошибка открытия файла %q:\n\t%v", filePath, err.Error())
		return nil, err
	}

	var Data map[string]*Data
	Decoder := gob.NewDecoder(file)

	err = Decoder.Decode(&Data)
	if err != nil {
		fmt.Printf("Ошибка десериализации:\n\t%v", err.Error())
		return nil, err
	}

	return Data, nil
}

//////////////////////////////////////////////////////////////////////////////

//////////////////////// Системные методы ////////////////////////////////////

func MergeData(inData map[string]*Data, outData map[string]*Data) {
	for k, value := range inData {
		if _, exist := outData[k]; exist {
			outData[k].value += value.value
			outData[k].count += value.count
		} else {
			outData[k] = value
		}
	}
}

func GetFiles(DirPath string) []string {
	var result []string
	f := func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || info.Size() == 0 {
			return nil
		} else {
			result = append(result, path)
		}

		return nil
	}

	filepath.Walk(DirPath, f)
	return result
}

func uuid() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("Error: ", err)
		return ""
	} else {
		return fmt.Sprintf("%X", b)
	}
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

func FindFiles(rootDir string) {

	start := time.Now()
	group := &sync.WaitGroup{}
	mergeGroup := &sync.WaitGroup{}
	resultGroup := &sync.WaitGroup{}
	mergeChan := make(chan map[string]*Data, Go*AddSizeChan)
	ResultChan := make(chan map[string]*Data, Go*AddSizeChan) // канал в который будет помещаться результат парсинга

	for i := 0; i < Go; i++ {
		mergeGroup.Add(1)
		go goMergeData(mergeChan, ResultChan, mergeGroup)
	}

	resultGroup.Add(1)
	go goPrettyPrint(ResultChan, resultGroup)

	for _, File := range GetFiles(rootDir) {
		if strings.HasSuffix(File, "log") {
			group.Add(1)
			go ParsFile(File, mergeChan, group)
		}
	}

	group.Wait()
	close(mergeChan)
	mergeGroup.Wait()
	close(ResultChan)
	resultGroup.Wait()

	elapsed := time.Now().Sub(start)
	fmt.Printf("Код выполнялся: %v\n", elapsed)
}

//////////////////////////////////////////////////////////////////////////////

//////////////////////////// Цепочки /////////////////////////////////////////

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

//////////////////////////////////////////////////////////////////////////////

func readStdIn() {
	mergeChan := make(chan map[string]*Data, Go*AddSizeChan)
	ResultChan := make(chan map[string]*Data, Go*AddSizeChan) // канал в который будет помещаться результат парсинга
	mergeGroup := &sync.WaitGroup{}
	resultGroup := &sync.WaitGroup{}

	for i := 0; i < Go; i++ {
		mergeGroup.Add(1)
		go goMergeData(mergeChan, ResultChan, mergeGroup)
	}

	resultGroup.Add(1)
	go goPrettyPrint(ResultChan, resultGroup)

	in := bufio.NewScanner(os.Stdin)
	ParsStream(in, ResultChan)

	close(mergeChan)
	mergeGroup.Wait()
	close(ResultChan)
	resultGroup.Wait()
}

func ParsStream(Scan *bufio.Scanner, mergeChan chan<- map[string]*Data) {

	inChan := make(chan *string, Go) // канал в который будет писаться исходные данные для парсенга
	//outChan := make(chan map[string]*Data, Go*100) // канал в который будет помещаться результат парсинга
	//dirChan := make(chan string, Go)           // канал в который будет писаться путь к временным директориям
	WriteGroup := &sync.WaitGroup{} // Група для ожидания завершения горутин работающих с каналом inChan
	//ReadGroup := &sync.WaitGroup{}  // Група для ожидания завершения горутин работающих с каналом outChan
	//ReadDirGroup := &sync.WaitGroup{}          // Група для ожидания завершения горутин работающих с каналом dirChan

	pattern := `(?mi)\d\d:\d\d\.\d+[-]\d+`
	re := regexp.MustCompile(pattern)

	//runWorkers(Go, inChan, outChan, WriteGroup)

	for i := 0; i < Go; i++ {
		WriteGroup.Add(1)
		go startWorker(inChan, mergeChan, WriteGroup)
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
	WriteGroup.Wait()
	//close(outChan)
	//ReadGroup.Wait()
	//close(dirChan)
	//ReadDirGroup.Wait()
}

func PrettyPrint(inData map[string]*Data) {
	// переводим map в массив
	len := len(inData)
	array := make([]*Data, len, len)
	i := 0
	for _, value := range inData {
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

///////////////////////// Legacy ///////////////////////////////////////////

func MergeFiles(DirPath string) {
	commonData := make(map[string]*Data)
	for _, filePath := range GetFiles(DirPath) {
		if Data, er := deSerialization(filePath); er == nil {
			MergeData(Data, commonData)
		}
	}

	SerializationAndSave(commonData, DirPath)

}

func MergeDirs(Dirs []string) string {
	commonData := make(map[string]*Data)

	for _, dir := range Dirs {
		for _, file := range GetFiles(dir) {
			if Data, er := deSerialization(file); er == nil {
				MergeData(Data, commonData)
			}
		}
		os.RemoveAll(dir)
	}
	TempDir, _ := ioutil.TempDir("", "")
	SerializationAndSave(commonData, TempDir)
	return TempDir
}

func goReaderDirChan(dirChan <-chan string, G *sync.WaitGroup) {
	defer G.Done()

	var Dirs []string
	for dir := range dirChan {
		Dirs = append(Dirs, dir)
	}
	MergeDirs(Dirs)
}

//////////////////////////////////////////////////////////////////////////////
