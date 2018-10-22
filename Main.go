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

	"runtime/pprof"
)

type Data struct {
	value  int64
	count  int
	OutStr string
}

type IChain interface {
	Execute(SourceStr string) (string, string, int64)
}

type ImapData interface {
	MergeData(inData ImapData)
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

type mapData map[string]*Data

const AddSizeChan = 10

var SortByCount, SortByValue, IO, v bool
var Top, Go int
var RootDir string

func main() {
	flag.BoolVar(&SortByCount, "SortByCount", false, "Сортировка по количеству вызовов (bool)")
	flag.BoolVar(&SortByValue, "SortByValue", false, "Сортировка по значению (bool)")
	flag.BoolVar(&IO, "io", false, "Флаг указывающий, что данные будут поступать из StdIn (bool)")
	//flag.BoolVar(&v, "v", false, "Флаг включающий вывод лога. Не используется при чтении данных из потока StdIn (bool)")
	flag.IntVar(&Top, "Top", 100, "Ограничение на вывод по количеству записей")
	flag.IntVar(&Go, "Go", 10, "Количество воркеров которые будут обрабатывать файл")
	flag.StringVar(&RootDir, "RootDir", "", "Корневая директория")

	cpuprofile := flag.Bool("cpuprof", false, "Профилирование CPU (bool)")
	memprofile := flag.Bool("memprof", false, "Профилирование памяти (bool)")
	flag.Parse()

	if *cpuprofile {
		StartCPUProf()
		defer pprof.StopCPUProfile()
	}
	if *memprofile {
		StartMemProf()
		defer pprof.StopCPUProfile()
	}

	FindFiles(`D:\1C_Log\Lazarenko\CALL_SCALL_BD`)
	return

	if RootDir != "" {
		FindFiles(RootDir)
	} else if IO {
		readStdIn()
	} else {
		panic("Не определены входящие данные")
	}

}

func readStdIn() {
	mergeChan := make(chan mapData, Go*AddSizeChan)
	mergeGroup := &sync.WaitGroup{}

	for i := 0; i < Go; i++ {
		go goMergeData(mergeChan, mergeGroup)
	}

	in := bufio.NewScanner(os.Stdin)
	ParsStream(in, BuildChain(), mergeChan)

	close(mergeChan)
	mergeGroup.Wait()
}

func ParsStream(Scan *bufio.Scanner, RepChain *Chain, mergeChan chan<- mapData) {

	inChan := make(chan *string, Go*2) // канал в который будет писаться исходные данные для парсенга
	WriteGroup := &sync.WaitGroup{}    // Група для ожидания завершения горутин работающих с каналом inChan
	//RepChain := ChainPool.Get().(*Chain) // Объект который будет парсить

	pattern := `(?mi)\d\d:\d\d\.\d+[-]\d+`
	re := regexp.MustCompile(pattern)

	for i := 0; i < Go; i++ {
		go startWorker(inChan, mergeChan, WriteGroup, RepChain)
	}

	buff := make([]string, 1)
	PushChan := func() {
		part := strings.Join(buff, "\n")
		inChan <- &part
		buff = nil // Очищаем
	}

	for ok := Scan.Scan(); ok == true; {
		txt := Scan.Text()
		if ok := re.MatchString(txt); ok {
			PushChan()
			buff = append(buff, txt)
		} else {
			// Если мы в этом блоке, значит у нас многострочное событие, накапливаем строки в буфер
			buff = append(buff, txt)
		}

		ok = Scan.Scan()
	}
	if len(buff) > 0 {
		PushChan()
	}

	close(inChan) // Закрываем канал на для чтения
	WriteGroup.Wait()
}

func PrettyPrint(inData mapData) {
	//fmt.Print("\n============================\n\n")

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

func ParsPart(Blob *string, RepChain IChain) mapData {
	Str := *Blob
	if Str == "" {
		return nil
	}
	//return make(mapData)

	key, data, value := RepChain.Execute(Str)
	/* 	key := ""
	   	data := ""
	   	var value int64 = 0 */
	result := Data{OutStr: data, value: value, count: 1}
	return mapData{getHash(key): &result}
}

//////////////////////////// Профилирование //////////////////////////////////

func StartCPUProf() {
	f, err := os.Create("cpu.out")
	//defer f.Close() нельзя иначе писаться не будет

	if err != nil {
		fmt.Println("Произошла ошибка при создании cpu.out: ", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Println("Не удалось запустить профилирование CPU: ", err)
	}
}

func StartMemProf() {
	f, err := os.Create("mem.out")
	//defer f.Close()

	if err != nil {
		fmt.Println("Произошла ошибка при создании mem.out: ", err)
	}

	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		fmt.Println("Не удалось запустить профилирование памяти: ", err)
	}
}

//////////////////////////////////////////////////////////////////////////////

//////////////////////////// Горутины ////////////////////////////////////////

func ReadInfoChan(infoChan <-chan int64, Fullsize *int64) {
	for size := range infoChan {
		fmt.Printf("\nОбработано %f%v", float64(size)/float64(*Fullsize)*100, `%`)
		//*Fullsize = atomic.AddInt64(Fullsize, -size)
	}
}

func goMergeData(outChan <-chan mapData, G *sync.WaitGroup) {
	G.Add(1)
	defer G.Done()

	var Data = make(mapData)
	for input := range outChan {
		Data.MergeData(input)
		runtime.Gosched() // Передаем управление другой горутине.
	}

	PrettyPrint(Data)
}

func goPrettyPrint(resultChan <-chan mapData, G *sync.WaitGroup) {
	G.Add(1)
	defer G.Done()

	var resultData = make(mapData)
	for input := range resultChan {
		resultData.MergeData(input)
	}

	PrettyPrint(resultData)
}

func startWorker(inChan <-chan *string, outChan chan<- mapData, group *sync.WaitGroup, Chain *Chain) {
	group.Add(1)
	defer group.Done()

	for input := range inChan {
		outChan <- ParsPart(input, Chain)
		runtime.Gosched() // Передаем управление другой горутине.
	}
}

func ParsFile(FilePath string, mergeChan chan<- mapData, infoChan chan<- int64, Chain *Chain, group *sync.WaitGroup) {
	defer group.Done()

	var file *os.File
	defer file.Close()
	file, er := os.Open(FilePath)
	/*info, _ := file.Stat()
	 if v {
		defer func() { infoChan <- info.Size() }()
	} */

	if er != nil {
		fmt.Printf("Ошибка открытия файла %q\n\t%v", FilePath, er.Error())
		return
	}

	ParsStream(bufio.NewScanner(file), Chain, mergeChan)
}

//////////////////////////////////////////////////////////////////////////////

///////////////////////// Сериализация ///////////////////////////////////////

func (d *mapData) SerializationAndSave(TempDir string) {

	file, err2 := os.Create(path.Join(TempDir, uuid()))
	defer file.Close()
	if err2 != nil {
		fmt.Println("Ошибка создания файла:\n", err2.Error())
		return
	}

	d.Serialization(file)
}

func (d *mapData) Serialization(outFile *os.File) {
	Encode := gob.NewEncoder(outFile)

	err := Encode.Encode(d)
	if err != nil {
		fmt.Println("Ошибка создания Encode:\n", err.Error())
		return
	}
}

func deSerialization(filePath string) (mapData, error) {
	var file *os.File
	file, err := os.Open(filePath)
	defer os.Remove(filePath)
	defer file.Close()

	if err != nil {
		fmt.Printf("Ошибка открытия файла %q:\n\t%v", filePath, err.Error())
		return nil, err
	}

	var Data mapData
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

func (this mapData) MergeData(inData mapData) {
	for k, value := range inData {
		if _, exist := this[k]; exist {
			this[k].value += value.value
			this[k].count += value.count
		} else {
			this[k] = value
		}
	}
}

func (this mapData) GetObject() mapData {
	return this
}

func GetFiles(DirPath string) ([]string, int64) {
	var result []string
	var size int64
	f := func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || info.Size() == 0 {
			return nil
		} else {
			result = append(result, path)
			size += info.Size()
		}

		return nil
	}

	filepath.Walk(DirPath, f)
	return result, size
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

	group := new(sync.WaitGroup)                    // Группа для горутин по файлам
	mergeGroup := new(sync.WaitGroup)               // Группа для горутин которые делают первичное объеденение
	mergeChan := make(chan mapData, Go*AddSizeChan) // Канал в который будут помещаться данные от пула воркеров, для объеденения
	infoChan := make(chan int64, 2)                 // Информационный канал, в него пишется размеры файлов
	Files, size := GetFiles(rootDir)
	Chain := BuildChain()

	for i := 0; i < Go; i++ {
		go goMergeData(mergeChan, mergeGroup)
	}
	//go goPrettyPrint(ResultChan, resultGroup)
	//if v {
	//	go ReadInfoChan(infoChan, &size)
	//}

	if v {
		fmt.Printf("Поиск файлов в каталоге %q, общий размер (%v kb)", rootDir, size/1024)
	}

	for _, File := range Files {
		if strings.HasSuffix(File, "log") {
			group.Add(1)
			go ParsFile(File, mergeChan, infoChan, Chain, group)
		}
	}

	group.Wait()
	close(mergeChan)
	close(infoChan)
	mergeGroup.Wait()

	elapsed := time.Now().Sub(start)
	fmt.Printf("Код выполнялся: %v\n", elapsed)
}

//////////////////////////////////////////////////////////////////////////////

//////////////////////////// Цепочки /////////////////////////////////////////

func BuildChain() *Chain {
	/* Element0 := Chain{
		regexp:         regexp.MustCompile(`(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Module=(?P<Module>[^,]+)(?:.+?)Method=(?P<Method>[^,]+)`),
		AgregateFileld: []string{"event", "DB", "Module", "Method"},
		OutPattern:     "(%DB%) CALL, количество - %count%, duration - %Value%\n%Module%.%Method%",
	}

	Element1 := Chain{
		regexp:         regexp.MustCompile(`(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,](?P<event>[^,]+)(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)(Context=(?P<Context>[^,]+))`),
		NextElement:    &Element0,
		AgregateFileld: []string{"event", "DB", "Context"},
		OutPattern:     "(%DB%) %event%, количество - %count%, duration - %Value%\n%Context%",
	} */

	Element1 := Chain{
		//preCondition:   func(In string) bool { return strings.Contains(In, ",CALL") },
		regexp:         regexp.MustCompile(`(?si)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Module=(?P<Module>[^,]+)(?:.+?)Method=(?P<Method>[^,]+)(?:.+?)MemoryPeak=(?P<Value>[\d]+)`),
		AgregateFileld: []string{"event", "DB", "Module", "Method"},
		OutPattern:     "(%DB%) CALL, количество - %count%, MemoryPeak - %Value%\n%Module%.%Method%",
	}

	Element2 := Chain{
		//preCondition:   func(In string) bool { return strings.Contains(In, ",CALL") },
		regexp:         regexp.MustCompile(`(?si)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Context=(?P<Context>[^,]+)(?:.+?)MemoryPeak=(?P<Value>[\d]+)`),
		NextElement:    &Element1,
		AgregateFileld: []string{"DB", "Context"},
		OutPattern:     "(%DB%) CALL, количество - %count%, MemoryPeak - %Value%\n%Context%",
	}

	Element3 := Chain{
		//preCondition:   func(In string) bool { return strings.Contains(In, ",EXCP") },
		regexp:         regexp.MustCompile(`(?si)[,]EXCP,(?:.*?)process=(?P<Process>[^,]+)(?:.*?)Descr=(?P<Context>[^,]+)`),
		NextElement:    &Element2,
		AgregateFileld: []string{"Process", "Context"},
		OutPattern:     "(%Process%) EXCP, количество - %count%\n%Context%",
	}
	return &Element3
}

func (c *Chain) Execute(SourceStr string) (string, string, int64) {
	exRegExp := func() map[string]string {
		/* if !c.preCondition(SourceStr) {
			return nil
		} */
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

///////////////////////// Legacy ///////////////////////////////////////////

func MergeFiles(DirPath string) {
	commonData := make(mapData)
	Files, _ := GetFiles(DirPath)
	for _, filePath := range Files {
		if Data, er := deSerialization(filePath); er == nil {
			commonData.MergeData(Data)
		}
	}

	commonData.SerializationAndSave(DirPath)

}

func MergeDirs(Dirs []string) string {
	commonData := make(mapData)

	for _, dir := range Dirs {
		Files, _ := GetFiles(dir)
		for _, file := range Files {
			if Data, er := deSerialization(file); er == nil {
				commonData.MergeData(Data)
			}
		}
		os.RemoveAll(dir)
	}
	TempDir, _ := ioutil.TempDir("", "")
	commonData.SerializationAndSave(TempDir)
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
