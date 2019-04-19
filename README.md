# GoParsLog_1C
Утилита для парса логов технологического журнала 1С

Настройка шаблона регулярки, агригации по полям можно производить в конфигурационном файле
```xml
<List> 
	<PatternsData>
		<!-- AgregateFileld поля по которым будет агригироваться значение из Value и count -->
		<AgregateFileld>DB</AgregateFileld>
		<AgregateFileld>Context</AgregateFileld>
		<OutPattern>(%DB%) CALL, количество - %count%, Memory - %Value%\n%Context%</OutPattern> <!-- Шаблон для вывода в консоль -->
		<RegexpPattern>(?si)[,]CALL(?:.*?)p:processName=(?P&lt;DB&gt;[^,]+)(?:.+?)Context=(?P&lt;Context&gt;[^,]+)(?:.+?)MemoryPeak=(?P&lt;Value&gt;[\d]+)</RegexpPattern>
	</PatternsData>
	<PatternsData>
		<!-- AgregateFileld поля по которым будет агригироваться значение из Value и count -->
		<AgregateFileld>Process</AgregateFileld>
		<AgregateFileld>Context</AgregateFileld>
		<OutPattern>(%Process%) EXCP, количество - %count%\n%Context%</OutPattern> <!-- Шаблон для вывода в консоль -->
		<RegexpPattern>(?si)[,]EXCP,(?:.*?)process=(?P&lt;Process&gt;[^,]+)(?:.*?)Descr=(?P&lt;Context&gt;[^,]+)</RegexpPattern>
	</PatternsData> 
</List> 
```
- RegexpPattern -шаблон регулярного выражения. Группы захвата обязательно должны быть именованными, в Go это делается так (?P<Имя> .....)
- AgregateFileld - имена групп захвата по которым будет производиться агрегация 
- OutPattern - шаблон по которому будет выводиться результат. В примере выше маркер %count% нигде не задается, это количество подходящих элементов в группе (при агрегации), давайте считать этот маркер "системным". Группа захвата содержащие значение которое будет суммироваться должна называться Value (имя групп регистрозависимое). Например, если мы захотим агрегировать значения duration, тогда регулярка будет такой `(?si)[\d]+:[\d]+\.[\d]+[-](?P<Value>[\d]+)[,]CALL(?:.*?)p:processName=(?P<DB>[^,]+)(?:.+?)........` -

### Пример вызова утилиты
    ParsLogs.exe -Top=10 -RootDir=D:\SessionData\callscall -SortByValue=true -confPath=conf.xml

Параметры которые принимает утилита:
* **-SortByCount** - признак того, что нужно сортировать результат по количеству 
* **-SortByValue** - признак того, что нужно сортировать по значению
* **-io** - признак того, что данные будут поступать из потока stdin
* **-Top** - ограничение по количеству выводимого результата
* **-Go** - количество горутин в пуле (по умолчанию 10)
* **-RootDir** - директория где будет осуществляться поиск 
* **-confPath** - конфигурационный xml файл 

Для профилирования:
* **-cpuprof** - профилирование CPU
* **-memprof** - профилирование памяти
 

Пример использования:

	ParsLogs.exe  -RootDir=C:\Logs

В данном случае поиск логов будет производиться по каталогу "C:\Logs"

Также можно применять в тандеме с grep'ом

	grep '' -rh --include '*.log' | ParsLogs.exe -io