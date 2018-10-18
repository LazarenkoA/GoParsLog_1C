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
	db_Name string
	context string
	event   string
	value   int64
	count   int
}

//type mapData map[string]*Data
var FullData map[string]*Data
var Mutex = &sync.Mutex{}
var SortByCount, SortByValue bool
var Top int

/* var PoolData = sync.Pool{
	New: func() interface{} {
		return make(mapData)
	},
} */

func main() {
	FullData = make(map[string]*Data)
	flag.BoolVar(&SortByCount, "SortByCount", false, "Сортировка по количеству вызовов (bool)")
	flag.BoolVar(&SortByValue, "SortByValue", false, "Сортировка по значению (bool)")
	flag.IntVar(&Top, "Top", 100, "ограничение на вывод по количеству записей (int)")
	flag.Parse()

	//FindFiles(`D:\1C_Log\КБР\1C_log`)
	FindFiles(`D:\1C_Log\Lazarenko`)
}

func runWorkers(count int, inChan <-chan *string, outChan chan<- map[string]*Data, group *sync.WaitGroup) {
	for i := 0; i < count; i++ {
		group.Add(1)
		go startWorker(inChan, outChan, group)
	}
}

func startWorker(inChan <-chan *string, outChan chan<- map[string]*Data, group *sync.WaitGroup) {
	defer group.Done()

	for input := range inChan {
		outChan <- ParsPart(input)
		runtime.Gosched() // Передаем управление другой горутине.
	}
}

func (d *Data) Sort() {

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
			//fmt.Printf("тек. каталог %q\n\tфайл %v\n", path, info.Name())
		}
		return nil
	}

	filepath.Walk(rootDir, callBack) // Поиск файлов.
	group.Wait()

	PrettyPrint()

	elapsed := time.Now().Sub(start)
	fmt.Printf("Код выполнялся: %v\n", elapsed)
}

func goReader(outChan <-chan map[string]*Data) {
	for input := range outChan {
		MergeData(input)
	}
}

func MergeData(Data map[string]*Data) {
	for key, value := range Data {
		//FData := PoolData.Get().(map[string]*Data)

		Mutex.Lock()
		if _, exist := FullData[key]; exist {
			FullData[key].value += value.value
			FullData[key].count += value.count
		} else {
			FullData[key] = value
		}
		Mutex.Unlock()
		//PoolData.Put(FData)
	}
}

func ParsFile(FilePath string, group *sync.WaitGroup) {
	defer group.Done()

	inChan := make(chan *string, 10)
	outChan := make(chan map[string]*Data, 10)
	localgroup := &sync.WaitGroup{} // Группа для ожидания выполнения пула воркеров.

	pattern := `(?mi)\d\d:\d\d\.\d+[-]\d+`
	re := regexp.MustCompile(pattern)

	var file *os.File
	defer file.Close()
	file, er := os.Open(FilePath)

	if er != nil {
		fmt.Printf("Ошибка открытия файла %v\n\t%v", FilePath, er.Error())
		return
	}

	runWorkers(10, inChan, outChan, localgroup)
	go goReader(outChan) // Отдельная горутина которая будет читать из канала.

	Scan := bufio.NewScanner(file)
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
	//data := PoolData.Get().(map[string]*Data)

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
		fmt.Printf("(%v) %v count - %v, duration - %v\n\t%v\n", array[id].db_Name, array[id].event, array[id].count, array[id].value, array[id].context)
	}
}

func ParsPart(Blob *string) map[string]*Data {
	Str := *Blob
	if Str == "" {
		return nil
	}

	exRegExp := func(pattern string) map[string]string {
		re := regexp.MustCompile(pattern)

		matches := re.FindStringSubmatch(Str)
		if len(matches) == 0 {
			return nil
		}

		// GroupsName - для работы с именоваными группами захвата.
		GroupsName := make(map[string]string)
		for id, name := range re.SubexpNames() {
			GroupsName[name] = matches[id]
		}

		return GroupsName
	}

	pattern := `(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,](?P<event>[^,]+)(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)(Context=(?P<Context>[^,]+))`
	Add_pattern := `(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,](?P<event>[^,]+)(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)Module=(?P<Module>[^,]+)(?:.+?)Method=(?P<Method>[^,]+)`
	var resultRegExp map[string]string

	if resultRegExp = exRegExp(pattern); resultRegExp == nil {
		resultRegExp = exRegExp(Add_pattern)
	}
	if resultRegExp == nil {
		return nil
	}
	var Context string
	if _, exist := resultRegExp["Context"]; exist {
		Context = resultRegExp["Context"]
	} else if _, exist := resultRegExp["Method"]; exist {
		Context = resultRegExp["Method"] + "." + resultRegExp["Method"]
	} else {
		return nil
	}

	result := Data{context: Context, db_Name: resultRegExp["DB"], event: resultRegExp["event"], count: 1}
	result.value, _ = strconv.ParseInt(resultRegExp["Value"], 10, 64)

	return map[string]*Data{getHash(result.context): &result}
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
