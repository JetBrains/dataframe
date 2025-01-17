//@file:ImportDataSchema(
//    "SearchResponse",
//    "src/main/resources/searchResponse.json",
//)
//@file:ImportDataSchema(
//    "StatisticsResponse",
//    "src/main/resources/statisticsResponse.json",
//)
//
//package org.jetbrains.kotlinx.dataframe.examples.youtube
//
//import kotlinx.datetime.Instant
//import org.jetbrains.kotlinx.dataframe.AnyFrame
//import org.jetbrains.kotlinx.dataframe.AnyRow
//import org.jetbrains.kotlinx.dataframe.DataRow
//import org.jetbrains.kotlinx.dataframe.annotations.ImportDataSchema
//import org.jetbrains.kotlinx.dataframe.api.*
//import org.jetbrains.kotlinx.dataframe.dataTypes.IFRAME
//import org.jetbrains.kotlinx.dataframe.dataTypes.IMG
//import org.jetbrains.kotlinx.dataframe.io.read
//import java.net.URL
//
//fun load(path: String) = DataRow.read("$basePath/$path&key=$apiKey")
//
//fun load(path: String, maxPages: Int): AnyFrame {
//    val rows = mutableListOf<AnyRow>()
//    var pagePath = path
//    do {
//        val row = load(pagePath)
//        rows.add(row)
//        val next = row.getValueOrNull<String>("nextPageToken")
//        pagePath = "$path&pageToken=$next"
//
//    } while (next != null && rows.size < maxPages)
//    return rows.concat()
//}
//
//fun main() {
//    val searchRequest = "cute%20cats"
//    val resultsPerPage = 50
//    val maxPages = 5
//
//    val videoId by column<String>("id")
//    val channel by columnGroup()
//
//    val videos = load("search?q=$searchRequest&maxResults=$resultsPerPage&part=snippet", maxPages)
//        .convertTo<SearchResponse> {
//            convert<String?>().with { it.toString() }
//            convert<Int?>().with { it ?: 0 }
//        }
//        .items.concat()
//        .dropNulls { id.videoId }
//        .select { id.videoId into videoId and snippet }
//        .distinct()
//        .parse()
//        .convert { colsAtAnyDepth().colsOf<URL>() }.with {
//            IMG(it, maxHeight = 150)
//        }.add("video") {
//            val id = videoId()
//            IFRAME("http://www.youtube.com/embed/$id")
//        }.move { snippet.title and snippet.publishTime }.toTop()
//        .move { snippet.channelId and snippet.channelTitle }.under(channel)
//        .remove { snippet }
//
//    val stats = videos[videoId]
//        .chunked(50)
//        .map {
//            val ids = it.joinToString("%2C")
//            load("videos?part=statistics&id=$ids").cast<StatisticsResponse>()
//        }.asColumnGroup()
//        .items.concat()
//        .select { id and statistics.allCols() }
//        .parse()
//
//    val withStat = videos.join(stats) { videoId match right.id }
//
//    val viewCount by column<Int>()
//    val publishTime by column<Instant>()
//
//    val channels = withStat
//        .groupBy { channel }.sum { viewCount }
//        .sortByDesc { viewCount }
//        .flatten()
//
//    channels.print(borders = true, columnTypes = true)
//
//    val growth = withStat
//        .select { publishTime and viewCount }
//        .convert { publishTime and viewCount }.toLong()
//        .sortBy { publishTime }
//        .cumSum { viewCount }
//
//    growth.print(borders = true, columnTypes = true)
//}
