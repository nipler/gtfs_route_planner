package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const MAX_DISTANCE_MOVE float64 = 500.00

const TIME_START_INTERVAL_1 int64 = 180
const TIME_END_INTERVAL_1 int64 = 180

const TIME_START_INTERVAL_2 int64 = 180
const TIME_END_INTERVAL_2 int64 = 180

var database *sql.DB
var debug int
var pathWithTwoRoutes2 []string
var routeOptions []string
var routeOptionsUnion []string
var routeOptionsNear []string

type RouteInfo struct {
	Id       string
	Name     string
	LongName string
	Type     int
	Dir      string
	Shape    string
}

type RouteIdRow struct {
	RouteId        string
	ShapeId        string
	RouteShortName string
	RouteLongName  string
	RouteType      int
}

type PathStopInfo struct {
	Start StopInfoPath `json:"start"`
	End   StopInfoPath `json:"end"`
}

type StopInfoPath struct {
	Id          int     `json:"id"`
	Cords       Coords  `json:"cords"`
	Cord        string  `json:"cord"`
	Name        string  `json:"name"`
	WalkTime    int     `json:"walk_time"`
	Distance    float64 `json:"distance"`
	ArrivalTime string  `json:"arrival_time"`
}

type PathResult struct {
	Id           string        `json:"id"`
	Name         string        `json:"name"`
	LongName     string        `json:"longname"`
	Type         string        `json:"type"`
	Direction    string        `json:"direction"`
	ShapeId      string        `json:"shape"`
	TypeRoute    string        `json:"type_route"`
	Stop         PathStopInfo  `json:"stop"`
	Stops        []StopInfo    `json:"stops"`
	Shapes       []ShapeCoords `json:"shapes"`
	RideTime     int           `json:"route_time"`
	RideDistance float64       `json:"ridedistance"`
}

func main() {

	var start_point string
	flag.StringVar(&start_point, "start_point", "", "a string var")

	var end_point string
	flag.StringVar(&end_point, "end_point", "", "a string var")

	var time_type int
	flag.IntVar(&time_type, "time_type", 0, "a int var")

	var time_his string
	flag.StringVar(&time_his, "time", "", "a string var")

	var date string
	flag.StringVar(&date, "date", "", "a string var")

	var route_type string
	flag.StringVar(&route_type, "route_type", "", "a string var")

	var change_time int
	flag.IntVar(&change_time, "change_time", 15, "a int var")

	var walk_max int
	flag.IntVar(&walk_max, "walk_max", 500, "a int var")

	var walk_speed float64
	flag.Float64Var(&walk_speed, "walk_speed", 4.00, "a float64 var")

	var routes string
	flag.StringVar(&routes, "routes", "", "a string var")

	var route_id string
	flag.StringVar(&route_id, "route_id", "", "a string var")

	flag.IntVar(&debug, "debug", 0, "a bool var")

	flag.Parse()

	startTime := makeTimestamp()
	if debug > 0 {
		fmt.Println("Start time:", startTime)
	}

	// fmt.Println("start_point:", start_point)
	// fmt.Println("end_point:",end_point)
	// fmt.Println("time_type:", time_type)
	// fmt.Println("time:", time)
	// fmt.Println("date:", date)
	// fmt.Println("route_type:", route_type)
	// fmt.Println("change_time:", change_time)
	// fmt.Println("walk_max:", walk_max)
	// fmt.Println("walk_speed:", walk_speed)
	// fmt.Println("routes:", routes)
	// fmt.Println("route_id:", route_id)

	//routeNms := strings.Split(routes, ",")

	db, err := sql.Open("mysql", "site_api:84hTgejf7663@tcp(localhost:3306)/gtfs")

	if err != nil {
		fmt.Println(err)
	}
	database = db
	defer db.Close()

	start_point_arr := strings.Split(start_point, ":")
	end_point_arr := strings.Split(end_point, ":")

	lat1, _ := strconv.ParseFloat(start_point_arr[0], 64)
	lon1, _ := strconv.ParseFloat(start_point_arr[1], 64)
	lat2, _ := strconv.ParseFloat(end_point_arr[0], 64)
	lon2, _ := strconv.ParseFloat(end_point_arr[1], 64)

	// fmt.Println("Переданные координаты:")

	// fmt.Println("lat1:", lat1)
	// fmt.Println("lon1:", lon1)
	// fmt.Println("lat2:", lat2)
	// fmt.Println("lon2:", lon2)

	// Ищем ближайшие остановки с переданным координатам

	type StopsMap struct {
		Name     string
		Cords    Coords
		Distance float64
		WalkTime int
		Routes   map[int]RouteInfo
	}

	type StartEnd struct {
		Start map[int]*StopsMap
		End   map[int]*StopsMap
	}

	carTypes := getTypes()

	result := StartEnd{}

	result.Start = make(map[int]*StopsMap)
	result.End = make(map[int]*StopsMap)

	distances_start := make(map[float64]int)
	distances_stop := make(map[float64]int)
	var kmStart []float64
	var kmEnd []float64

	stops := getStops()

	stepTime1 := makeTimestamp()
	if debug > 0 {
		fmt.Println("Time stops:", stepTime1-startTime)
	}

	// Прокрутим все остановки и найдем ближайшие к нашим координатам
	for i := 0; i < len(stops); i++ {
		lat3 := stops[i].Lat
		lon3 := stops[i].Lon

		km1 := Distance(lat1, lon1, lat3, lon3)
		km2 := Distance(lat2, lon2, lat3, lon3)

		max_dist := 1000.00

		// Если идти от точки назначения до остановки дольше константы
		if km1 <= max_dist {

			distances_start[km1] = stops[i].Id

			result.Start[stops[i].Id] = &StopsMap{
				Name:     stops[i].Name,
				Cords:    Coords{Lat: lat3, Lon: lon3},
				Distance: km1,
				WalkTime: int(((km1 / 1000) / walk_speed) * 60 * 60),
				Routes:   make(map[int]RouteInfo)}
			kmStart = append(kmStart, km1)
		}

		// Если идти от конечной до точки назначения дольше константы
		if km2 <= max_dist {

			distances_stop[km2] = stops[i].Id
			result.End[stops[i].Id] = &StopsMap{
				Name:     stops[i].Name,
				Cords:    Coords{Lat: lat3, Lon: lon3},
				Distance: km2,
				WalkTime: int(((km2 / 1000) / walk_speed) * 60 * 60),
				Routes:   make(map[int]RouteInfo)}
			kmEnd = append(kmEnd, km2)
		}
	}

	// fmt.Println(distances_start)
	// fmt.Println(distances_stop)

	stepTime2 := makeTimestamp()
	if debug > 0 {
		fmt.Println("Time while stops:", stepTime2-stepTime1)
		fmt.Println("Найдно ближайших остановок:", len(kmStart), len(kmEnd))
	}

	var find_routes []string

	sort.Float64s(kmStart)

	// Пробежимся по остановкам начальной точки и посмотрим какие маршруты там проходят
	for s := 0; s < len(kmStart); s++ {

		stop_id := distances_start[kmStart[s]]

		routes_onstop := getStopRoutes(stop_id, route_type, route_id, date)

		if len(routes_onstop) == 0 {
			continue
		}

		for k := 0; k < len(routes_onstop); k++ {
			if !inArray(find_routes, routes_onstop[k].Id) {
				find_routes = append(find_routes, routes_onstop[k].Id)
			}
		}

		// Не нужно включать остановку если у нее теже самые маршруты
		flagIsset := false
		for _, startStopsRow := range result.Start {
			var findFlag int
			for r := 0; r < len(startStopsRow.Routes); r++ {
				for k := 0; k < len(routes_onstop); k++ {
					if routes_onstop[k].Shape == startStopsRow.Routes[r].Shape {
						findFlag++
					}
				}
			}
			if findFlag == len(routes_onstop) {
				flagIsset = true
			}
		}

		if !flagIsset {

			result.Start[stop_id].Routes = routes_onstop

			if s >= 15 {
				break
			}
		}

	}

	stepTime3 := makeTimestamp()
	if debug > 0 {
		fmt.Println("Time while starts:", stepTime3-stepTime2)
	}

	sort.Float64s(kmEnd)

	for s := 0; s < len(kmEnd); s++ {
		stop_id := distances_stop[kmEnd[s]]

		routes_onstop := getStopRoutes(stop_id, route_type, route_id, date)

		if len(routes_onstop) == 0 {
			continue
		}

		for k := 0; k < len(routes_onstop); k++ {
			if !inArray(find_routes, routes_onstop[k].Id) {
				find_routes = append(find_routes, routes_onstop[k].Id)
			}
		}

		// Не нужно включать остановку если у нее теже самые маршруты
		flagIsset := false
		for _, endStopsRow := range result.End {
			var findFlag int
			for r := 0; r < len(endStopsRow.Routes); r++ {
				for k := 0; k < len(routes_onstop); k++ {
					if routes_onstop[k].Shape == endStopsRow.Routes[r].Shape {
						findFlag++
					}
				}
			}
			if findFlag == len(routes_onstop) {
				flagIsset = true
			}
		}

		if !flagIsset {
			result.End[stop_id].Routes = routes_onstop

			if s >= 15 {
				break
			}
		}

	}

	stepTime4 := makeTimestamp()
	if debug > 0 {
		fmt.Println("Time while ends:", stepTime4-stepTime3)
		fmt.Println("Получаем остановки маршрутов:", len(find_routes))
	}

	stopsRouteMapById, stopsRouteMapBySeq := getAllStopForRoutes(find_routes, date)

	if debug > 0 {
		fmt.Println("Начинаем искать общие маршруты:")
	}
	// Перменная в которую все записываем
	var findPathResult [][]*PathResult

	// Все найденные маршруты для последующего получения шейпов
	// var findRoutes []string

	// Все прямые маршруты
	var directRoutes []string
	// Шейпы маршрутов
	var routesShape []string
	// Остановки маршрутов
	var routeStops []int

	stepTime5 := makeTimestamp()
	if debug > 0 {
		fmt.Println("Start find direction:", stepTime5-stepTime4)
	}

	// Поиск прямых маршрутов без пересадок
	for s := 0; s < len(kmStart); s++ {
		startId := distances_start[kmStart[s]]
		startStopsMap := result.Start[startId]
		routesStart := startStopsMap.Routes

		for e := 0; e < len(kmEnd); e++ {
			endId := distances_stop[kmEnd[e]]
			endStopsMap := result.End[endId]
			routesEnd := endStopsMap.Routes

			union, routeData := findUnionRoutes(routesStart, routesEnd)

			if len(union) > 0 {
				for a := 0; a < len(union); a++ {

					var ok1, ok2 bool
					if routeData[a].Dir == "a-b" {
						_, ok1 = stopsRouteMapById[union[a]].AB[startId]
						_, ok2 = stopsRouteMapById[union[a]].AB[endId]
					} else {
						_, ok1 = stopsRouteMapById[union[a]].BA[startId]
						_, ok2 = stopsRouteMapById[union[a]].BA[endId]
					}

					// Если найдено в АБ
					if ok1 && ok2 {

						if !inArray(directRoutes, union[a]) {

							var unionStops map[int]StopInfo
							var unionStopsSeq map[int]StopInfo
							if routeData[a].Dir == "a-b" {
								unionStops = stopsRouteMapById[union[a]].AB
								unionStopsSeq = stopsRouteMapBySeq[union[a]].AB
							} else {
								unionStops = stopsRouteMapById[union[a]].BA
								unionStopsSeq = stopsRouteMapBySeq[union[a]].BA
							}

							if unionStops[startId].Sequence < unionStops[endId].Sequence {

								// findRoutes = append(findRoutes, union[a])
								directRoutes = append(directRoutes, union[a])
								routesShape = append(routesShape, routeData[a].Shape)
								routeStops = append(routeStops, startId)
								routeStops = append(routeStops, endId)

								stopsPath, _ := prepareRouteStops(unionStopsSeq, startId, endId)

								startStop := StopInfoPath{Id: startId,
									Cords:       startStopsMap.Cords,
									Cord:        floatCordsToString(startStopsMap.Cords.Lat, startStopsMap.Cords.Lon),
									Name:        startStopsMap.Name,
									WalkTime:    startStopsMap.WalkTime,
									Distance:    startStopsMap.Distance,
									ArrivalTime: ""}

								endStop := StopInfoPath{Id: endId,
									Cords:       endStopsMap.Cords,
									Cord:        floatCordsToString(endStopsMap.Cords.Lat, endStopsMap.Cords.Lon),
									Name:        endStopsMap.Name,
									WalkTime:    endStopsMap.WalkTime,
									Distance:    endStopsMap.Distance,
									ArrivalTime: ""}

								var row []*PathResult

                                directionMod := "1";
                                if routeData[a].Dir == "a-b" {
                                    directionMod = "0";
                                }
								row = append(row, &PathResult{Id: union[a],
									Name:         routeData[a].Name,
									LongName:     routeData[a].LongName,
									Type:         carTypes[routeData[a].Type],
									Direction:    directionMod,
									ShapeId:      routeData[a].Shape,
									TypeRoute:    "direct",
									Stop:         PathStopInfo{Start: startStop, End: endStop},
									Stops:        stopsPath,
									Shapes:       []ShapeCoords{},
									RideTime:     0,
									RideDistance: 0.00})

								findPathResult = append(findPathResult, row)

							}
						}
					}

				}
			}
		}
	}

	// В эту переменную записываем все найденые машруты с пересадками
	//var pathWithTwoRoutes []string

	if len(directRoutes) == 0 {
		// Если ничего не нашлось с прямым маршрутом то ищем с общей остановкой

		for h := 0; h < len(kmStart); h++ {
			startStopId := distances_start[kmStart[h]]
			startStopsMap := result.Start[startStopId]
			routesStart := startStopsMap.Routes

			for e := 0; e < len(kmEnd); e++ {
				endStopId := distances_stop[kmEnd[e]]
				endStopsMap := result.End[endStopId]
				routesEnd := endStopsMap.Routes

				unionStops := findUnionStops(directRoutes, routesStart, routesEnd, startStopId, endStopId, stopsRouteMapById)
				if len(unionStops) > 0 {
					//fmt.Println("unionStops:",unionStops)
					for s := 0; s < len(unionStops); s++ {
						stopData := unionStops[s].stopData
						routeFirst := unionStops[s].routeFirst
						DirFirst := unionStops[s].DirFirst
						routeSecond := unionStops[s].routeSecond
						DirSecond := unionStops[s].DirSecond

						routesShape = append(routesShape, routeFirst.Shape)
						routesShape = append(routesShape, routeSecond.Shape)

						routeStops = append(routeStops, startStopId)
						routeStops = append(routeStops, stopData.Id)
						routeStops = append(routeStops, endStopId)

						var row []*PathResult

						var stopsPath1 []StopInfo

						if DirFirst == "a-b" {
							stopsPath1, _ = prepareRouteStops(stopsRouteMapBySeq[routeFirst.Id].AB, startStopId, stopData.Id)
						} else {
							stopsPath1, _ = prepareRouteStops(stopsRouteMapBySeq[routeFirst.Id].BA, startStopId, stopData.Id)
						}
						// Первый маршрут
						startStop1 := StopInfoPath{Id: startStopId,
							Cords:       startStopsMap.Cords,
							Cord:        floatCordsToString(startStopsMap.Cords.Lat, startStopsMap.Cords.Lon),
							Name:        startStopsMap.Name,
							WalkTime:    startStopsMap.WalkTime,
							Distance:    startStopsMap.Distance,
							ArrivalTime: ""}

						endStop1 := StopInfoPath{Id: stopData.Id,
							Cords:       Coords{Lat: stopData.Lat, Lon: stopData.Lon},
							Cord:        floatCordsToString(stopData.Lat, stopData.Lon),
							Name:        stopData.Name,
							WalkTime:    0,
							Distance:    0,
							ArrivalTime: ""}

                        directionMod := "1"
                        if DirFirst == "a-b" {
                            directionMod = "0"
                        }
						row = append(row, &PathResult{Id: routeFirst.Id,
							Name:         routeFirst.Name,
							LongName:     routeFirst.LongName,
							Type:         carTypes[routeFirst.Type],
							Direction:    directionMod,
							ShapeId:      routeFirst.Shape,
							TypeRoute:    "direct",
							Stop:         PathStopInfo{Start: startStop1, End: endStop1},
							Stops:        stopsPath1,
							Shapes:       []ShapeCoords{},
							RideTime:     0,
							RideDistance: 0.00})

						// Второй маршрут
						var stopsPath2 []StopInfo

						if DirSecond == "a-b" {
							stopsPath2, _ = prepareRouteStops(stopsRouteMapBySeq[routeSecond.Id].AB, stopData.Id, endStopId)
						} else {
							stopsPath2, _ = prepareRouteStops(stopsRouteMapBySeq[routeSecond.Id].BA, stopData.Id, endStopId)
						}

						startStop2 := StopInfoPath{Id: stopData.Id,
							Cords:       Coords{Lat: stopData.Lat, Lon: stopData.Lon},
							Cord:        floatCordsToString(stopData.Lat, stopData.Lon),
							Name:        stopData.Name,
							WalkTime:    0,
							Distance:    0,
							ArrivalTime: ""}

						endStop2 := StopInfoPath{Id: endStopId,
							Cords:       endStopsMap.Cords,
							Cord:        floatCordsToString(endStopsMap.Cords.Lat, endStopsMap.Cords.Lon),
							Name:        endStopsMap.Name,
							WalkTime:    endStopsMap.WalkTime,
							Distance:    endStopsMap.Distance,
							ArrivalTime: ""}

                        directionMod = "1"
                        if DirSecond == "a-b" {
                            directionMod = "0"
                        }
						row = append(row, &PathResult{Id: routeSecond.Id,
							Name:         routeSecond.Name,
							LongName:     routeSecond.LongName,
							Type:         carTypes[routeSecond.Type],
							Direction:    directionMod,
							ShapeId:      routeSecond.Shape,
							TypeRoute:    "direct",
							Stop:         PathStopInfo{Start: startStop2, End: endStop2},
							Stops:        stopsPath2,
							Shapes:       []ShapeCoords{},
							RideTime:     0,
							RideDistance: 0.00})

						findPathResult = append(findPathResult, row)

					}
				}
			}
		}

		// Если ничего не нашлось с прямым маршрутом то ищем с пересадкой
		for h := 0; h < len(kmStart); h++ {
			startStopId := distances_start[kmStart[h]]
			startStopsMap := result.Start[startStopId]
			routesStart := startStopsMap.Routes

			for e := 0; e < len(kmEnd); e++ {
				endStopId := distances_stop[kmEnd[e]]
				endStopsMap := result.End[endStopId]
				routesEnd := endStopsMap.Routes

				unionStops := findNearStopsForRoutes(directRoutes, routesStart, routesEnd, startStopId, endStopId, stopsRouteMapById)

				if len(unionStops) > 0 {

					for s := 0; s < len(unionStops); s++ {

						stopDataFirst := unionStops[s].First.Stop
						routeFirst := unionStops[s].First.Route
						DirFirst := unionStops[s].First.Dir
						Distance := unionStops[s].First.Distance
						stopDataSecond := unionStops[s].Second.Stop
						routeSecond := unionStops[s].Second.Route
						DirSecond := unionStops[s].Second.Dir

						routesShape = append(routesShape, routeFirst.Shape)
						routesShape = append(routesShape, routeSecond.Shape)
						routeStops = append(routeStops, startStopId)
						routeStops = append(routeStops, stopDataFirst.Id)
						routeStops = append(routeStops, stopDataSecond.Id)
						routeStops = append(routeStops, endStopId)

						var row []*PathResult

						//fmt.Println(stopData, routeFirst, DirFirst, routeSecond, DirSecond)
						var stopsPath1 []StopInfo

						if DirFirst == "a-b" {
							stopsPath1, _ = prepareRouteStops(stopsRouteMapBySeq[routeFirst.Id].AB, startStopId, stopDataFirst.Id)
						} else {
							stopsPath1, _ = prepareRouteStops(stopsRouteMapBySeq[routeFirst.Id].BA, startStopId, stopDataFirst.Id)
						}
						// Первый маршрут
						startStop1 := StopInfoPath{Id: startStopId,
							Cords:       startStopsMap.Cords,
							Cord:        floatCordsToString(startStopsMap.Cords.Lat, startStopsMap.Cords.Lon),
							Name:        startStopsMap.Name,
							WalkTime:    startStopsMap.WalkTime,
							Distance:    startStopsMap.Distance,
							ArrivalTime: ""}

						endStop1 := StopInfoPath{Id: stopDataFirst.Id,
							Cords:       Coords{Lat: stopDataFirst.Lat, Lon: stopDataFirst.Lon},
							Cord:        floatCordsToString(stopDataFirst.Lat, stopDataFirst.Lon),
							Name:        stopDataFirst.Name,
							WalkTime:    0,
							Distance:    0,
							ArrivalTime: ""}
                        directionMod := "1"
                        if DirFirst == "a-b" {
                            directionMod = "0"
                        }
						row = append(row, &PathResult{Id: routeFirst.Id,
							Name:         routeFirst.Name,
							LongName:     routeFirst.LongName,
							Type:         carTypes[routeFirst.Type],
							Direction:    directionMod,
							ShapeId:      routeFirst.Shape,
							TypeRoute:    "direct",
							Stop:         PathStopInfo{Start: startStop1, End: endStop1},
							Stops:        stopsPath1,
							Shapes:       []ShapeCoords{},
							RideTime:     0,
							RideDistance: 0.00})

						// Второй маршрут
						var stopsPath2 []StopInfo

						if DirSecond == "a-b" {
							stopsPath2, _ = prepareRouteStops(stopsRouteMapBySeq[routeSecond.Id].AB, stopDataSecond.Id, endStopId)
						} else {
							stopsPath2, _ = prepareRouteStops(stopsRouteMapBySeq[routeSecond.Id].BA, stopDataSecond.Id, endStopId)
						}

						startStop2 := StopInfoPath{Id: stopDataSecond.Id,
							Cords:       Coords{Lat: stopDataSecond.Lat, Lon: stopDataSecond.Lon},
							Cord:        floatCordsToString(stopDataSecond.Lat, stopDataSecond.Lon),
							Name:        stopDataSecond.Name,
							WalkTime:    int(((Distance / 1000) / walk_speed) * 60 * 60),
							Distance:    Distance,
							ArrivalTime: ""}

						endStop2 := StopInfoPath{Id: endStopId,
							Cords:       endStopsMap.Cords,
							Cord:        floatCordsToString(endStopsMap.Cords.Lat, endStopsMap.Cords.Lon),
							Name:        endStopsMap.Name,
							WalkTime:    endStopsMap.WalkTime,
							Distance:    endStopsMap.Distance,
							ArrivalTime: ""}

                        directionMod = "1"
                        if DirSecond == "a-b" {
                            directionMod = "0"
                        }
						row = append(row, &PathResult{Id: routeSecond.Id,
							Name:         routeSecond.Name,
							LongName:     routeSecond.LongName,
							Type:         carTypes[routeSecond.Type],
							Direction:    directionMod,
							ShapeId:      routeSecond.Shape,
							TypeRoute:    "direct",
							Stop:         PathStopInfo{Start: startStop2, End: endStop2},
							Stops:        stopsPath2,
							Shapes:       []ShapeCoords{},
							RideTime:     0,
							RideDistance: 0.00})

						// Исключим маршрут если надо проехать только одну остановку

						if len(stopsPath2) > 2 && len(stopsPath1) > 2 {
							flg := false
							if inArray(routeOptionsUnion, strings.Join([]string{routeFirst.Shape, routeSecond.Shape}, "-")) {
								for x := 0; x < len(findPathResult); x++ {
									if len(findPathResult[x]) > 1 {
										if (findPathResult[x][0].ShapeId == routeFirst.Shape) && (findPathResult[x][1].ShapeId == routeSecond.Shape) {
											// Если количество остановок в варианте c общей остановкой больше чем с пересадкой то удалим вариант
											if (len(findPathResult[x][0].Stops) + len(findPathResult[x][1].Stops)) > (len(stopsPath1) + len(stopsPath2)) {
												findPathResult = RemoveIndex(findPathResult, x)
											} else {
												flg = true
											}
										}
									}
								}
							}

							if !flg {
								findPathResult = append(findPathResult, row)
							}
						}
					}
				}
			}
		}
	}

	stepTime6 := makeTimestamp()
	if debug > 0 {
		fmt.Println("End find directions:", stepTime6-stepTime5)
	}

	// Получим шейпы для всех маршрутов
	stepTime7 := makeTimestamp()
	var routeShapes map[string]ShapeDirs
	if len(routesShape) > 0 {
		routeShapes = getAllShapesByRoute(routesShape)

		if debug > 0 {
			fmt.Println("Time find all shapes:", stepTime7-stepTime6)
		}
	}

	distances := make(map[int]float64)
	times := make(map[int]int)
	var distancesForSort []float64
	var timesForSort []int
	// Обрежем и присвоим шейпы маршрутам
	for p := 0; p < len(findPathResult); p++ {
		ways := findPathResult[p]
		var distance float64
		var time_sec int
		var trips []map[int]map[int]int
		for w := 0; w < len(ways); w++ {
			routeItem := ways[w]
			//fmt.Println(routeItem.Direction, routeItem.ShapeId)

			var shapeMap map[int]ShapeCoords
			if routeItem.Direction == "0" {
				shapeMap = routeShapes[routeItem.Id].AB
			} else {
				shapeMap = routeShapes[routeItem.Id].BA
			}
			findPathResult[p][w].Shapes, findPathResult[p][w].RideDistance = prepareRouteShapes(shapeMap, routeItem.Stop.Start.Cords, routeItem.Stop.End.Cords, routeItem.Id)
			distance += routeItem.RideDistance + routeItem.Stop.Start.Distance + routeItem.Stop.Start.Distance

			trips = append(trips, getTripsByStopsAndRoute(routeItem.Stop.Start.Id, routeItem.Stop.End.Id, routeItem.ShapeId, time_type, date, time_his))
		}
		var prevTime int

		for t := 0; t < len(trips); t++ {
			stopStartId := findPathResult[p][t].Stop.Start.Id
			stopEndId := findPathResult[p][t].Stop.End.Id

			flag := false

			// Отсортируем по дате во возрастанию
			timeSliceStart := sortMapByValue(trips[t][stopStartId])
			timeSliceEnd := sortMapByValue(trips[t][stopEndId])

			if t == 1 {
				prevTime = int(Strtotime(strings.Join([]string{date, findPathResult[p][0].Stop.End.ArrivalTime + ":00"}, " ")))
			}

			for a := 0; a < len(timeSliceStart); a++ {
				for b := 0; b < len(timeSliceEnd); b++ {
					tripIdStart := trips[t][stopStartId][timeSliceStart[a]]
					tripIdEnd := trips[t][stopEndId][timeSliceEnd[b]]

					if (tripIdEnd == tripIdStart) && (timeSliceStart[a] < timeSliceEnd[b]) {

						if t == 1 {
							if prevTime < timeSliceStart[a] {

							} else {
								continue
							}
						}

						findPathResult[p][t].Stop.Start.ArrivalTime = time.Unix(int64(timeSliceStart[a]), 0).UTC().Format("15:04")
						findPathResult[p][t].Stop.End.ArrivalTime = time.Unix(int64(timeSliceEnd[b]), 0).UTC().Format("15:04")

						findPathResult[p][t].RideTime = timeSliceEnd[b] - timeSliceStart[a]

						time_sec += findPathResult[p][t].RideTime + findPathResult[p][t].Stop.Start.WalkTime + findPathResult[p][t].Stop.End.WalkTime

						flag = true
						break
					}
				}
				if flag {
					break
				}
			}

		}

		/* for w := 0; w < len(ways); w++ {
			routeItem := ways[w]
			fmt.Println(trips[w])


		} */

		distancesForSort = append(distancesForSort, distance)
		timesForSort = append(timesForSort, time_sec)
		distances[p] = distance
		times[p] = time_sec
	}

	sort.Float64s(distancesForSort)
	sort.Ints(timesForSort)
	var findPathResultSorted [][]*PathResult

	/* for d := 0; d < len(distancesForSort); d++ {
		for index, dist := range distances {
			if distancesForSort[d] == dist {
				findPathResultSorted = append(findPathResultSorted, findPathResult[index])
				delete(distances, index)
				break
			}
		}
	} */

	for d := 0; d < len(timesForSort); d++ {
		for index, dist := range times {
			if timesForSort[d] == dist {
				findPathResultSorted = append(findPathResultSorted, findPathResult[index])
				delete(times, index)
				break
			}
		}
	}

	stepTime8 := makeTimestamp()
	if debug > 0 {
		fmt.Println("Done:", stepTime8-stepTime7)
	}
	data, err := json.Marshal(findPathResultSorted)

	if debug == 0 {
		fmt.Printf("%s\n", data)
	}

	return
}

func sortMapByValue(trips map[int]int) []int {

	var resultTime []int
	var resultTrip []int
	for timeStamp, tripId := range trips {
		resultTime = append(resultTime, int(timeStamp))
		resultTrip = append(resultTrip, tripId)
	}

	sort.Ints(resultTime)

	return resultTime
}

func findUnionRoutes(routes1, routes2 map[int]RouteInfo) ([]string, []RouteInfo) {
	//fmt.Println(routes1, routes2)
	var union []string
	var info []RouteInfo
	for a := 0; a < len(routes1); a++ {
		for b := 0; b < len(routes2); b++ {

			if !inArray(routeOptions, strings.Join([]string{routes1[a].Shape, routes2[b].Shape}, "-")) {
				routeOptions = append(routeOptions, strings.Join([]string{routes1[a].Shape, routes2[b].Shape}, "-"))

				if routes1[a].Shape == routes2[b].Shape {
					//fmt.Println(routes1[a].Shape)
					if !inArray(union, routes1[a].Id) {
						union = append(union, routes1[a].Id)
						info = append(info, routes1[a])
					}
				}
			}
		}
	}

	return union, info
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)

}

type UnionStop struct {
	stopData    StopInfo
	routeFirst  RouteInfo
	DirFirst    string
	routeSecond RouteInfo
	DirSecond   string
}

// Функция ищет общую остановку у двух маршрутов
func findUnionStops(findRoutes []string, routesStart, routesEnd map[int]RouteInfo, startId, endId int, stopsRouteMapById map[string]DirStops) []UnionStop {

	var result []UnionStop

	for a := 0; a < len(routesStart); a++ {

		// Пропускаем если это общий маршрут
		flagS := false
		for f := 0; f < len(findRoutes); f++ {
			if findRoutes[f] == routesStart[a].Id {
				flagS = true
				break
			}
		}
		if flagS {
			continue
		}

		startDir := routesStart[a].Dir

		// Остановки начального маршрута
		var stopsRouteStart map[int]StopInfo
		if startDir == "a-b" {
			stopsRouteStart = stopsRouteMapById[routesStart[a].Id].AB
		} else {
			stopsRouteStart = stopsRouteMapById[routesStart[a].Id].BA
		}
		// Позиция начальной остановки в маршруте
		startSeq := stopsRouteStart[startId].Sequence

		// Перебираем маршруты конечной остановки
		for b := 0; b < len(routesEnd); b++ {

			// Пропускаем если это общий маршрут
			flagE := false
			for f := 0; f < len(findRoutes); f++ {
				if findRoutes[f] == routesEnd[b].Id {
					flagE = true
					break
				}
			}

			if flagE {
				continue
			}

			// Если маршруты равны то пропустим
			if routesStart[a].Id == routesEnd[b].Id {
				continue
			}

			if !inArray(routeOptionsUnion, strings.Join([]string{routesStart[a].Shape, routesEnd[b].Shape}, "-")) {
				routeOptionsUnion = append(routeOptionsUnion, strings.Join([]string{routesStart[a].Shape, routesEnd[b].Shape}, "-"))

				endDir := routesEnd[b].Dir

				// Остановки конечного маршрута
				var stopsRouteEnd map[int]StopInfo
				if endDir == "a-b" {
					stopsRouteEnd = stopsRouteMapById[routesEnd[b].Id].AB
				} else {
					stopsRouteEnd = stopsRouteMapById[routesEnd[b].Id].BA
				}
				// Позиция конечной остановки в маршруте
				endSeq := stopsRouteEnd[endId].Sequence

				// Перебираем остановки начального маршрута и ищем такие же остановки в конечном маршруте
				for stopUnionId, stopUnionRow := range stopsRouteStart {
					stopRow, ok1 := stopsRouteEnd[stopUnionId]

					// Позиция остановки на первом маршруте
					startUnionSeq := stopUnionRow.Sequence
					// Позиция остановки на конечном маршруте
					endUnionSeq := stopRow.Sequence

					if ok1 && endSeq > endUnionSeq && startSeq < startUnionSeq {
						if !inArrayKey(result, routesStart[a].Id, routesEnd[b].Id) {
							result = append(result, UnionStop{stopData: stopRow, routeFirst: routesStart[a], DirFirst: startDir, routeSecond: routesEnd[b], DirSecond: endDir})
						}
					}
				}
			}
		}
	}

	return result
}

type NearStop struct {
	First  NearItem
	Second NearItem
}

type NearItem struct {
	Stop     StopInfo
	Route    RouteInfo
	Dir      string
	Distance float64
}

type StopList struct {
	Start int
	End   int
}

func floatCordsToString(lat, lon float64) string {
    // to convert a float number to a string


    return strings.Join([]string{strconv.FormatFloat(lat, 'f', 6, 64), strconv.FormatFloat(lon, 'f', 6, 64)}, ":")
}

// Функция ищет общую две ближайшие остановки у двух маршрутов
func findNearStopsForRoutes(findRoutes []string, routesStart, routesEnd map[int]RouteInfo, startId, endId int, stopsRouteMapById map[string]DirStops) []NearStop {

	var result []NearStop

	for a := 0; a < len(routesStart); a++ {

		// Пропускаем если это общий маршрут
		flagS := false
		for f := 0; f < len(findRoutes); f++ {
			if findRoutes[f] == routesStart[a].Id {
				flagS = true
				break
			}
		}
		if flagS {
			continue
		}

		startDir := routesStart[a].Dir

		var structRouteStart map[int]StopInfo
		if startDir == "a-b" {
			structRouteStart = stopsRouteMapById[routesStart[a].Id].AB
		} else {
			structRouteStart = stopsRouteMapById[routesStart[a].Id].BA
		}

		startSeq := structRouteStart[startId].Sequence

		var stopsStart []int
		for stopIdStart, _ := range structRouteStart {
			/* dist := Distance(stopStartRow.Lat, stopStartRow.Lon, stopEndRow.Lat, stopEndRow.Lon)
			if (MAX_DISTANCE_MOVE >= dist) && (dist > 0) {
				findDist = append(findDist, dist)
				findStops[dist] = StopList{Start: stopIdStart, End: stopEndId}

			} */
			stopsStart = append(stopsStart, stopIdStart)
		}

		// В каждой остановке перебираем конечные маршруты
		for b := 0; b < len(routesEnd); b++ {

			// Пропускаем если это общий маршрут
			flagE := false
			for f := 0; f < len(findRoutes); f++ {
				if findRoutes[f] == routesEnd[b].Id {
					flagE = true
					break
				}
			}

			if flagE {
				continue
			}

			// Если маршруты равны то пропустим
			if routesStart[a].Id == routesEnd[b].Id {
				continue
			}

			// Запишем сюда все уже проверенные варианты
			if !inArray(routeOptionsNear, strings.Join([]string{routesStart[a].Shape, routesEnd[b].Shape}, "-")) {
				routeOptionsNear = append(routeOptionsNear, strings.Join([]string{routesStart[a].Shape, routesEnd[b].Shape}, "-"))

				endDir := routesEnd[b].Dir

				var structRouteEnd map[int]StopInfo
				if endDir == "a-b" {
					structRouteEnd = stopsRouteMapById[routesEnd[b].Id].AB
				} else {
					structRouteEnd = stopsRouteMapById[routesEnd[b].Id].BA
				}

				endSeq := structRouteEnd[endId].Sequence

				// var findDist []float64
				// findStops := make(map[float64]StopList)
				// Перебираем конечные остановки
				var stopsEnd []int
				for stopEndId, _ := range structRouteEnd {
					// Перебираем остановки начального маршрута
					stopsEnd = append(stopsEnd, stopEndId)
				}

				//sort.Float64s(findDist)

				findStops, findDist := getDistances(stopsStart, stopsEnd)

				if len(findDist) > 0 {

					index := 0

					var seqStartMin int //Максимаьно маленький
					var seqEndMin int   //Максимально большой

					//Задача найти в первом маршруте максимально маленький seq а во втором максимально большой
					for f := 0; f < len(findDist); f++ {
						if findDist[f] <= 250.00 {
							stopIdFirst_ := findStops[findDist[f]].Start
							stopIdSecond_ := findStops[findDist[f]].End

							stopRowFirstSeq := structRouteStart[stopIdFirst_].Sequence
							stopRowSecondSeq := structRouteEnd[stopIdSecond_].Sequence

							if f == 0 {
								seqStartMin = stopRowFirstSeq
								seqEndMin = stopRowSecondSeq
							}

							if (stopRowFirstSeq <= seqStartMin) && (stopRowSecondSeq >= seqEndMin) {
								index = f
								seqStartMin = stopRowFirstSeq
								seqEndMin = stopRowSecondSeq
							}
						}
					}

					stopIdFirst := findStops[findDist[index]].Start
					stopIdSecond := findStops[findDist[index]].End
					stopRowFirst := structRouteStart[stopIdFirst]
					stopRowSecond := structRouteEnd[stopIdSecond]

					if endSeq > stopRowSecond.Sequence && startSeq < stopRowFirst.Sequence {

						if !inArrayStruct(result, routesStart[a].Id, routesEnd[b].Id) {
							result = append(result, NearStop{First: NearItem{Stop: stopRowFirst, Route: routesStart[a], Dir: startDir, Distance: findDist[index]}, Second: NearItem{Stop: stopRowSecond, Route: routesEnd[b], Dir: endDir, Distance: 0.00}})
						}
					}
				}
			}
		}
	}

	return result
}

// Call
// -start_point="47.2185079:47.2185079" -end_point="47.2094307:39.7385178" -time_type=0 -time="12:27" -date="2019-05-13" -route_type="704,1501,900,800,999,701,100" -change_time=15 -walk_max=500 -walk_speed=4 -routes="" -route_id=""
