# GoParsLog_1C
Утилита для парса логов технологического журнала 1С

Новые шаблоны регулярных выражений добавляются в метод BuildChain() пакета Tools
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
   


Работать с цепочкой нужно через sync.Pool **ChainPool**

Параметры которые принимает утилита:

* **-SortByCount** - признак того, что нужно сортировать результат по количеству 
* **-SortByValue** - признак того, что нужно сортировать по значению
* **-io** - признак того, что данные будут поступать из потока stdin
* **-Top** - ограничение по количеству выводимого результата
* **-Go** - количество горутин в пуле (по умолчанию 10)
* **-RootDir** - директория где будет осуществляться поиск 

И для профилирования:

* **-cpuprof** - профилирование CPU
* **-memprof** - профилирование памяти
 

Пример использования:

	ParsLogs.exe  -RootDir=C:\Logs

В данном случае поиск логов будет производиться по каталогу "C:\Logs"

Также можно применять в тандеме с grep'ом

	grep '' -rh --include '*.log' | ParsLogs.exe -io

Пример сочетания параметров

	ParsLogs.exe  -RootDir=C:\Logs -Top=10 -SortByCount

Будет выведено 10 результатов отсортированных по количеству
