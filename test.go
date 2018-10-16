package main

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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

func main() {
	//var FilePath string = `D:\1C_Log\КБР\1C_log\CALL_DB\rphost_6056\18100514_.log`
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

	elapsed := time.Now().Sub(start)
	fmt.Printf("Код выполнялся: %v\n", elapsed)
}

func goReader(outChan <-chan map[string]*Data) {
	for input := range outChan {
		PrettyPrint(input)
	}
}

func ParsFile(FilePath string, group *sync.WaitGroup) {
	defer group.Done()

	//data := make(map[string]*Data)
	inChan := make(chan *string, 10)
	outChan := make(chan map[string]*Data, 10)
	localgroup := &sync.WaitGroup{} // Группа для ожидания выполнения пула воркеров.

	pattern := `(?mi)\d\d:\d\d\.\d+[-]\d+`
	re := regexp.MustCompile(pattern)

	var file *os.File
	defer file.Close()
	if file, er := os.Open(FilePath); er != nil {
		fmt.Printf("Ошибка открытия файла %v\n\t%v", FilePath, er.Error())
	} else {
		runWorkers(10, inChan, outChan, localgroup)
		go goReader(outChan) // Отдельная горутина которая будет читать из канала.

		Scan := bufio.NewScanner(file)
		buff := make([]string, 1)

		for ok := Scan.Scan(); ok == true; {
			txt := Scan.Text()
			if ok := re.MatchString(txt); ok {
				part := strings.Join(buff, "\n")
				inChan <- &part

				/* for input := range outChan {
					fmt.Printf("%#v", input)
				} */

				//ParsPart(&part, data) // Отправляем в обработку строки от предыдущих итераций.
				buff = nil
				buff = append(buff, txt)
			} else {
				// Если мы в этом блоке, значит у нас многострочное событие, накапливаем строки в буфер
				buff = append(buff, txt)
			}

			ok = Scan.Scan()
		}
		close(inChan)

		localgroup.Wait()
		close(outChan)
		//PrettyPrint(data)
	}

}

func PrettyPrint(data map[string]*Data) {
	for _, value := range data {
		fmt.Printf("(%v) %v count - %v, duration - %v\n\t%v\n", value.db_Name, value.event, value.count, value.value, value.context)
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

	/* v := strings.Index(result.context, "ПолучитьПредставленияСсылок")
	if v > 0 {
		fmt.Print(v)
	} */
	//Hash := getHash(result.event + result.context)

	// Если inData уже есть с таким хешем, тогда склидываем value
	/* 	if _, exist := inData[Hash]; exist {
	   		inData[Hash].value += inData[Hash].value
	   		inData[Hash].count++
	   	} else {
	   		inData[Hash] = &result
	   	} */

	return map[string]*Data{getHash(result.context): &result}

	/* if re.MatchString(Str) {
		for _, submatches := range re.FindAllStringSubmatchIndex(Str, -1) {
			DB, Context, event, value := []byte{}, []byte{}, []byte{}, []byte{}
			DB = re.ExpandString(DB, "$DB", Str, submatches)
			Context = re.ExpandString(Context, "$Context", Str, submatches)
			event = re.ExpandString(event, "$event", Str, submatches)
			value = re.ExpandString(value, "$Value", Str, submatches)
			valueInt, _ := strconv.ParseInt(string(value), 10, 64)

			v := Data{Context: string(Context),
				value:   valueInt,
				DB_Name: string(DB)}

			fmt.Println(v)
		}
	} */
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
