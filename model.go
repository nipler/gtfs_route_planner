package main

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func inArray(list []string, str string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

func RemoveIndex(result [][]*PathResult, index int) [][]*PathResult {
	result[index] = result[len(result)-1] // Copy last element to index i.
	return result[:len(result)-1]         // Truncate slice.

}

func inArrayKey(list []UnionStop, str1, str2 string) bool {
	for _, v := range list {
		if v.routeFirst.Id == str1 && v.routeSecond.Id == str2 {
			return true
		}
	}
	return false
}

func inArrayStruct(list []NearStop, str1, str2 string) bool {
	for _, v := range list {

		if (v.First.Route.Id == str1) && (v.Second.Route.Id == str2) {
			return true
		}
	}
	return false
}

type Coords struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Stop struct {
	Id   int     `json:"id"`
	Name string  `json:"name"`
	Lat  float64 `json:"lat"`
	Lon  float64 `json:"lon"`
}

func getStops() []Stop {

	rows, err := database.Query("SELECT stop_id,  stop_name, stop_lat, stop_lon FROM `picas_stops`")
	if err != nil {
		//fmt.Println(err)
	}
	defer rows.Close()

	stops := []Stop{}

	for rows.Next() {
		p := Stop{}

		err := rows.Scan(&p.Id, &p.Name, &p.Lat, &p.Lon)
		if err != nil {
			//fmt.Println(err)
			continue
		}

		stops = append(stops, p)
	}

	return stops

}

func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

func getStopRoutes(id int, types string, routes string, date string) map[int]RouteInfo {

	if len(date) == 0 {
		currentTime := time.Now()
		date = currentTime.Format("2006-01-02")
	}
	weekday := time.Now().Weekday().String()

	query2 := fmt.Sprintf("SELECT trip_id FROM picas_stop_times WHERE stop_id = %v ", id)

	rows2, err2 := database.Query(query2)
	//fmt.Println("q:", q)
	if err2 != nil {
		//fmt.Println(err2)
	}
	defer rows2.Close()

	var tripIds []int

	for rows2.Next() {
		var trip int
		err := rows2.Scan(&trip)
		if err != nil {
			//fmt.Println(err)
			continue
		}
		tripIds = append(tripIds, trip)
	}

	query := "SELECT " +
		"DISTINCT picas_trips.route_id, " +
		"picas_trips.shape_id, " +
		"picas_routes.route_short_name, " +
		"picas_routes.route_long_name, " +
		"picas_routes.route_type " +
		"FROM picas_trips " +
		"LEFT JOIN picas_calendar ON picas_trips.service_id = picas_calendar.service_id " +
		"LEFT JOIN picas_routes ON picas_routes.route_id = picas_trips.route_id " +
		"WHERE picas_trips.trip_id IN (%v) "

	if len(types) > 0 {
		routeTypes := strings.Split(types, ",")
		query += fmt.Sprintf("AND picas_routes.route_type IN (%v) ", strings.Join(routeTypes[:], ","))
	}
	if len(routes) > 0 {
		routeIds := strings.Split(routes, ",")
		query += fmt.Sprintf("AND picas_trips.route_id IN (%v) ", strings.Join(routeIds[:], ","))
	}

	query += "AND picas_calendar.%v = 1 " +
		"AND DATE(picas_calendar.start_date) <= '%v' " +
		"AND DATE(picas_calendar.end_date) >= '%v' " +
		"AND (picas_trips.shape_id LIKE '%v' OR picas_trips.shape_id LIKE '%v') "

	q := fmt.Sprintf(query, sqlIntSeq(tripIds), strings.ToLower(weekday), date, date, "%a-b", "%b-a")
	//fmt.Println(q)
	rows, err := database.Query(q)

	if err != nil {
		//fmt.Println(err)
	}
	defer rows.Close()

	var res = make(map[int]RouteInfo)
	var match_ab = regexp.MustCompile(`^(.+?)_(a[0-9]*\-b[0-9]*)(.*)$`)
	var match_ba = regexp.MustCompile(`^(.+?)_(b[0-9]*\-a[0-9]*)(.*)$`)
	var r = 0
	for rows.Next() {

		row := RouteIdRow{}
		err := rows.Scan(&row.RouteId, &row.ShapeId, &row.RouteShortName, &row.RouteLongName, &row.RouteType)
		if err != nil {
			//fmt.Println(err)
			continue
		}

		direction := ""

		if match_ba.MatchString(row.ShapeId) {
			direction = "b-a"
		} else if match_ab.MatchString(row.ShapeId) {
			direction = "a-b"
		}

		res[r] = RouteInfo{Id: row.RouteId, Name: row.RouteShortName, LongName: row.RouteLongName, Type: row.RouteType, Dir: direction, Shape: row.ShapeId}
		r++
	}

	return res
}

type CarTypes struct {
	Id   int
	Name string
}

func getTypes() map[int]string {
	rows, err := database.Query("SELECT picas_type, name_short_en FROM `car_type`")
	if err != nil {
		//fmt.Println(err)
	}
	defer rows.Close()

	types := make(map[int]string)

	for rows.Next() {
		p := CarTypes{}

		err := rows.Scan(&p.Id, &p.Name)
		if err != nil {
			//fmt.Println(err)
			continue
		}

		types[p.Id] = p.Name
	}

	return types
}

type TripsMap struct {
	TripId  int
	RouteId string
	ShapeId string
}

type StopInfo struct {
	Id       int     `json:"id"`
	Name     string  `json:"name"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	Sequence int     `json:"sequence"`
}

type RoutesStopRow struct {
	Id           int
	Name         string
	Lat          float64
	Lon          float64
	ArrivalTime  string
	Timestamp    string
	StopSequence int
	TripId       int
}

type DirStops struct {
	AB map[int]StopInfo
	BA map[int]StopInfo
}

func getAllStopForRoutes(routes []string, date string) (map[string]DirStops, map[string]DirStops) {

	if len(date) == 0 {
		currentTime := time.Now()
		date = currentTime.Format("2006-01-02")
	}
	weekday := time.Now().Weekday().String()

	query1 := "SELECT picas_trips.trip_id, picas_trips.route_id, picas_trips.shape_id " +
		"FROM picas_trips " +
		"LEFT JOIN picas_calendar ON picas_trips.service_id = picas_calendar.service_id " +
		"WHERE 1=1 "
	if len(routes) > 0 {
		routeIds := sqlStringSeq(routes)
		query1 += fmt.Sprintf("AND picas_trips.route_id IN (%v) ", strings.Join(routeIds[:], ","))
	}
	query1 += "AND picas_calendar.%v = 1 " +
		"AND DATE(picas_calendar.start_date) <= '%v' " +
		"AND DATE(picas_calendar.end_date) >= '%v' " +
		"AND (picas_trips.shape_id LIKE '%v' OR picas_trips.shape_id LIKE '%v') " +
		"GROUP BY picas_trips.shape_id " +
		"ORDER BY picas_trips.shape_id"

	q1 := fmt.Sprintf(query1, strings.ToLower(weekday), date, date, "%a-b", "%b-a")

	rows1, err1 := database.Query(q1)

	if err1 != nil {
		//fmt.Println(err1)
	}
	defer rows1.Close()

	var tripIds []int
	trips := make(map[int]TripsMap)

	for rows1.Next() {
		row1 := TripsMap{}
		err := rows1.Scan(&row1.TripId, &row1.RouteId, &row1.ShapeId)
		if err != nil {
			//fmt.Println(err)
			continue
		}
		//fmt.Println(row1.ShapeId)
		trips[row1.TripId] = row1
		tripIds = append(tripIds, row1.TripId)
	}

	query2 := "SELECT picas_stops.stop_id AS id, picas_stops.stop_name AS name, " +
		"picas_stops.stop_lat, picas_stops.stop_lon, picas_stop_times.arrival_time, " +
		"UNIX_TIMESTAMP(picas_stop_times.arrival_time) AS timestamp, picas_stop_times.stop_sequence, picas_stop_times.trip_id " +
		"FROM picas_stop_times " +
		"LEFT JOIN picas_stops ON picas_stops.stop_id = picas_stop_times.stop_id " +
		"WHERE 1=1 "
	query2 += fmt.Sprintf("AND picas_stop_times.trip_id IN (%v) ", sqlIntSeq(tripIds))
	query2 += "GROUP BY picas_stop_times.trip_id, picas_stop_times.stop_sequence " +
		"ORDER BY picas_stop_times.stop_sequence ASC "

	rows2, err2 := database.Query(query2)
	//fmt.Println("q:", q)
	if err2 != nil {
		//fmt.Println(err2)
	}
	defer rows2.Close()

	bySequence := make(map[string]DirStops)
	stopsInfo := make(map[string]DirStops)

	for rows2.Next() {

		row2 := RoutesStopRow{}
		err := rows2.Scan(&row2.Id, &row2.Name, &row2.Lat, &row2.Lon, &row2.ArrivalTime, &row2.Timestamp, &row2.StopSequence, &row2.TripId)
		if err != nil {
			//fmt.Println(err)
			continue
		}
		var match_ab = regexp.MustCompile(`^(.+?)_(a[0-9]*\-b[0-9]*)(.*)$`)
		var match_ba = regexp.MustCompile(`^(.+?)_(b[0-9]*\-a[0-9]*)(.*)$`)

		routeId := trips[row2.TripId].RouteId
		shapeId := trips[row2.TripId].ShapeId

		// Инциализируем карты
		if _, ok := stopsInfo[routeId]; ok {

		} else {

			stopsInfo[routeId] = DirStops{AB: make(map[int]StopInfo), BA: make(map[int]StopInfo)}
			bySequence[routeId] = DirStops{AB: make(map[int]StopInfo), BA: make(map[int]StopInfo)}
		}

		p := StopInfo{
			Id:       row2.Id,
			Name:     row2.Name,
			Lat:      row2.Lat,
			Lon:      row2.Lon,
			Sequence: row2.StopSequence}

		if match_ba.MatchString(shapeId) {
			stopsInfo[routeId].BA[row2.Id] = p
			bySequence[routeId].BA[row2.StopSequence] = p
		} else if match_ab.MatchString(shapeId) {
			stopsInfo[routeId].AB[row2.Id] = p
			bySequence[routeId].AB[row2.StopSequence] = p
		}

	}

	return stopsInfo, bySequence

}

func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	x := 2 * r * math.Asin(math.Sqrt(h))

	return x
}

/* func Distance(lat1, lng1, lat2, lng2 float64) float64 {
	const PI float64 = 3.141592653589793

	radlat1 := float64(PI * lat1 / 180)
	radlat2 := float64(PI * lat2 / 180)

	theta := float64(lng1 - lng2)
	radtheta := float64(PI * theta / 180)

	dist := math.Sin(radlat1)*math.Sin(radlat2) + math.Cos(radlat1)*math.Cos(radlat2)*math.Cos(radtheta)

	if dist > 1 {
		dist = 1
	}

	dist = math.Acos(dist)
	dist = dist * 180 / PI
	dist = dist * 60 * 1.1515

	dist = dist * 1.609344 * 1000

	return dist
} */

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func sqlIntSeq(ns []int) string {
	if len(ns) == 0 {
		return ""
	}

	// Appr. 3 chars per num plus the comma.
	estimate := len(ns) * 4
	b := make([]byte, 0, estimate)
	// Or simply
	//   b := []byte{}
	for _, n := range ns {
		b = strconv.AppendInt(b, int64(n), 10)
		b = append(b, ',')
	}
	b = b[:len(b)-1]
	return string(b)
}

func sqlStringSeq(ns []string) []string {

	var str []string
	for i := 0; i < len(ns); i++ {
		str = append(str, "'"+ns[i]+"'")
	}

	return str
}

func prepareRouteStops(routeStops map[int]StopInfo, stopStartId, stopEndId int) ([]StopInfo, int) {
	//fmt.Println(routeStops, stopStartId, stopEndId)
	var routeTime int
	var stops []StopInfo

	var keys []int
	for k := range routeStops {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	var flag bool

	for k := 0; k < len(keys); k++ {
		stopRow := routeStops[keys[k]]

		if stopRow.Id == stopStartId {
			flag = true
		}

		if flag {
			stops = append(stops, stopRow)
		}

		if stopRow.Id == stopEndId {
			flag = false
		}
	}

	return stops, routeTime
}

type ShapeRow struct {
	Id       string
	Lat      float64
	Lon      float64
	Sequence int
}

type ShapeCoords struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type ShapeDirs struct {
	AB map[int]ShapeCoords
	BA map[int]ShapeCoords
}

func getAllShapesByRoute(shapes_id []string) map[string]ShapeDirs {
	shapes_id = sqlStringSeq(shapes_id)

	// shapes_id := getShapesByTrips(routeIds)

	sql := fmt.Sprintf("SELECT picas_shapes.shape_id, picas_shapes.shape_pt_lat AS lat, picas_shapes.shape_pt_lon AS lon, picas_shapes.shape_pt_sequence FROM `picas_shapes` WHERE picas_shapes.shape_id IN (%v) ORDER BY picas_shapes.shape_pt_sequence ASC ", strings.Join(shapes_id[:], ","))

	rows, err := database.Query(sql)
	if err != nil {
		//fmt.Println(err)
	}
	defer rows.Close()

	var match_ab = regexp.MustCompile(`^(.+?)_(a[0-9]*\-b[0-9]*)(.*)$`)
	var match_ba = regexp.MustCompile(`^(.+?)_(b[0-9]*\-a[0-9]*)(.*)$`)
	regRouteAB, err := regexp.Compile(`_(a[0-9]*\-b[0-9]*)(.*)$`)
	regRouteBA, err := regexp.Compile(`_(b[0-9]*\-a[0-9]*)(.*)$`)

	result := make(map[string]ShapeDirs)
	//var routeId string
	for rows.Next() {
		p := ShapeRow{}

		err := rows.Scan(&p.Id, &p.Lat, &p.Lon, &p.Sequence)
		if err != nil {
			//fmt.Println(err)
			continue
		}

		if match_ba.MatchString(p.Id) {
			routeId := regRouteBA.ReplaceAllString(p.Id, "")

			// Инциализируем карты
			if _, ok := result[routeId]; ok {

			} else {

				result[routeId] = ShapeDirs{AB: make(map[int]ShapeCoords), BA: make(map[int]ShapeCoords)}
			}
			result[routeId].BA[p.Sequence] = ShapeCoords{Lat: p.Lat, Lon: p.Lon}

		} else if match_ab.MatchString(p.Id) {
			routeId := regRouteAB.ReplaceAllString(p.Id, "")

			// Инциализируем карты
			if _, ok := result[routeId]; ok {

			} else {

				result[routeId] = ShapeDirs{AB: make(map[int]ShapeCoords), BA: make(map[int]ShapeCoords)}
			}
			result[routeId].AB[p.Sequence] = ShapeCoords{Lat: p.Lat, Lon: p.Lon}
		}

	}

	return result

}

func getShapesByTrips(routeIds []string) []string {

	sql := fmt.Sprintf("SELECT shape_id FROM `picas_trips` WHERE route_id IN (%v) GROUP BY shape_id", strings.Join(routeIds[:], ","))

	rows, err := database.Query(sql)
	if err != nil {
		//fmt.Println(err)
	}
	defer rows.Close()

	var shapes []string

	for rows.Next() {
		var shape_id string

		err := rows.Scan(&shape_id)
		if err != nil {
			//fmt.Println(err)
			continue
		}

		shapes = append(shapes, "'"+shape_id+"'")
	}

	return shapes
}

func getDistances(stopsStart, stopsEnd []int) (map[float64]StopList, []float64) {

	sql := fmt.Sprintf("SELECT distance, stop_start_id, stop_end_id FROM `stops_distance` WHERE stop_start_id IN (%v) AND stop_end_id IN (%v)", sqlIntSeq(stopsStart), sqlIntSeq(stopsEnd))

	rows, err := database.Query(sql)
	if err != nil {
		//fmt.Println(err)
	}
	defer rows.Close()

	var distances []float64

	findStops := make(map[float64]StopList)

	for rows.Next() {
		var distance float64
		var stopIdStart int
		var stopEndId int

		err := rows.Scan(&distance, &stopIdStart, &stopEndId)
		if err != nil {
			//fmt.Println(err)
			continue
		}

		distances = append(distances, distance)
		findStops[distance] = StopList{Start: stopIdStart, End: stopEndId}
	}

	sort.Float64s(distances)

	return findStops, distances
}

type TripsRow struct {
	Count int
	Trips string
}

type TripItem struct {
	Time   string
	StopId int
	TripId int
}

func getTripsByStopsAndRoute(stopStartId, stopEndId int, shapeId string, time_type int, date, time_his string) map[int]map[int]int {

	// Если конечная переваливает за следующие сутки
	var betweens []string
	dateTime := strings.Join([]string{date, time_his}, " ")
	// Преобразуем в Unixtimestamp
	strtime := Strtotime(dateTime)

	if time_type == 0 { // Считаем по времени отправления
		// Прибавляем константу
		endTime := (strtime + TIME_START_INTERVAL_1*60)
		// Преобразуем в строку
		endUTCDate := time.Unix(endTime, 0).UTC().Format("2006-01-02")
		startUTCTime := time_his
		endUTCTime := time.Unix(endTime, 0).UTC().Format("15:04:05")

		// Если в результате получается что со смещением меняется дата
		if endUTCDate != date {
			betweens = append(betweens, " (picas_stop_times.departure_time >= '"+startUTCTime+"' AND picas_stop_times.departure_time <= '23:59:59')")
			betweens = append(betweens, " (picas_stop_times.departure_time >= '00:00:00' AND picas_stop_times.departure_time <= '09:00:00')")
		} else {
			betweens = append(betweens, " (picas_stop_times.departure_time >= '"+startUTCTime+"' AND picas_stop_times.departure_time <= '"+endUTCTime+"')")
		}
	} else { // Считаем по времени прибытия
		// Вычитаем константу
		startTime := (strtime - TIME_START_INTERVAL_1*60)
		// Преобразуем в строку
		startUTCDate := time.Unix(startTime, 0).UTC().Format("2006-01-02")
		endUTCTime := time_his
		startUTCTime := time.Unix(startTime, 0).UTC().Format("15:04:05")

		// Если в результате получается что со смещением меняется дата
		if startUTCDate != date {
			betweens = append(betweens, " (picas_stop_times.departure_time >= '"+startUTCTime+"' AND picas_stop_times.departure_time <= '23:59:59')")
			betweens = append(betweens, " (picas_stop_times.departure_time >= '00:00:00' AND picas_stop_times.departure_time <= '09:00:00')")
		} else {
			betweens = append(betweens, " (picas_stop_times.departure_time >= '"+startUTCTime+"' AND picas_stop_times.departure_time <= '"+endUTCTime+"')")
		}
	}

	stops := []int{stopStartId, stopEndId}
	trips := make(map[int]map[int]int)
	for b := 0; b < len(betweens); b++ {
		sql := fmt.Sprintf("SELECT COUNT(picas_trips.trip_id) as count, "+
			"GROUP_CONCAT(picas_stop_times.departure_time, '/', picas_stop_times.stop_id, '/', picas_trips.trip_id ORDER BY picas_stop_times.stop_sequence ASC) as trips "+
			"FROM picas_trips "+
			"LEFT JOIN picas_stop_times ON picas_stop_times.trip_id = picas_trips.trip_id "+
			"WHERE picas_stop_times.stop_id IN (%v) "+
			"AND  "+betweens[b]+
			"AND picas_trips.shape_id = '%v' "+
			"GROUP BY picas_trips.trip_id "+
			"HAVING COUNT(picas_trips.trip_id) = 2 "+
			"ORDER BY picas_stop_times.departure_time ASC", sqlIntSeq(stops), shapeId)

		rows, err := database.Query(sql)
		if err != nil {
			//fmt.Println(err)
		}
		defer rows.Close()

		for rows.Next() {
			trip := TripsRow{}

			err := rows.Scan(&trip.Count, &trip.Trips)

			//fmt.Println(tripsRow)
			if err != nil {
				//fmt.Println(err)
				continue
			}

			tripsRow := strings.Split(trip.Trips, ",")
			for t := 0; t < len(tripsRow); t++ {
				tripItem := strings.Split(tripsRow[t], "/")

				StopId, err := strconv.Atoi(tripItem[1])
				if err != nil {
					panic(err)
				}

				TripId, err := strconv.Atoi(tripItem[2])
				if err != nil {
					panic(err)
				}

				if _, ok := trips[StopId]; ok {

				} else {
					trips[StopId] = make(map[int]int)
				}
				dateTime := strings.Join([]string{date, tripItem[0]}, " ")

				trips[StopId][int(Strtotime(dateTime))] = TripId
			}
		}
	}

	return trips
}

var EPSILON float64 = 0.00001

func floatEquals(a, b float64) bool {

	if (a-b) < EPSILON && (b-a) < EPSILON {
		return true
	}
	return false
}

func prepareRouteShapes(shapes map[int]ShapeCoords, stopStartCord, stopEndCord Coords, routeId string) ([]ShapeCoords, float64) {
	//fmt.Println(routeId, len(shapes))
	pointsStart := -1
	pointsEnd := -1
	var keys []int
	for k := range shapes {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for i := 0; i < len(keys); i++ {
		shape := shapes[keys[i]]
		//fmt.Println(shape.Lat, stopStartCord.Lat, shape.Lon, stopStartCord.Lon)
		if floatEquals(shape.Lat, stopStartCord.Lat) && floatEquals(shape.Lon, stopStartCord.Lon) {
			pointsStart = i
		}

		if floatEquals(shape.Lat, stopEndCord.Lat) && floatEquals(shape.Lon, stopEndCord.Lon) {
			pointsEnd = i
		}
	}
	var distance float64
	var shapesCut []ShapeCoords
	var prevCoords ShapeCoords
	if pointsStart >= 0 && pointsEnd >= 0 {

		flag := false

		for i := 0; i < len(keys); i++ {

			if pointsStart == i {
				flag = true
			}
			if flag {
				//shapesCut[seq] = shapes[keys[i]]

				shapesCut = append(shapesCut, shapes[keys[i]])
				if (prevCoords.Lat != 0) && (prevCoords.Lon != 0) {
					d := Distance(prevCoords.Lat, prevCoords.Lon, shapes[keys[i]].Lat, shapes[keys[i]].Lon)
					distance = distance + d
				}

				prevCoords = shapes[keys[i]]
			}
			if pointsEnd == i {
				flag = false
			}
		}
	}

	return shapesCut, distance
}

func checkRoute(route1, route2 string, pathWithTwoRoutes []string) bool {
	for i := 0; i < len(pathWithTwoRoutes); i++ {

		if (pathWithTwoRoutes[i] == strings.Join([]string{route1, route2}, "-")) || (pathWithTwoRoutes[i] == strings.Join([]string{route2, route1}, "-")) {
			return true
		}
	}
	return false
}

func Strtotime(str string) int64 {

	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, str)
	if err != nil {
		return 0
	}
	return t.Unix()
}
