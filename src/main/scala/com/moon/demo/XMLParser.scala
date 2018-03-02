package com.moon.demo

/**
  * Created by mars on 2017/6/25.
  */
object XMLParser {
  def main(args: Array[String]): Unit = {
    val weather =
      <rss>
        <channel>
          <title>Yahoo! Weather - Boulder, CO</title>
          <item>
            <title>Conditions for Boulder, CO at 2:54 pm MST</title>
            <forecast day="Thu" date="10 Nov 2011" low="37" high="58" text="Partly Cloudy"
                      code="29" />
          </item>
        </channel>
      </rss>

    val forecast = weather \ "channel" \ "item" \ "forecast"

    val day = forecast \ "@day"     // Thu
    val date = forecast \ "@date"   // 10 Nov 2011
    val low = forecast \ "@low"     // 37
    val high = forecast \ "@high"   // 58
    val text = forecast \ "@text"   // Partly Cloudy

    val title = weather \ "channel" \ "title" text;
    print(title)

    print(day, date, low, high, text)
  }

  class XMLParser {

  }
}
