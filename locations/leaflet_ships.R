library(shiny)
library(leaflet)

# read the result of spark
df <- read.table("WP4/locations/results.csv", header = FALSE, sep = ",", col.names=c("lat", "lng", "date", "count"))

dates = unique(df$date)



shinyApp(
	ui = fluidPage(
		fluidRow(
			leafletOutput('map', height= 700)
		),
		# controls:
		#    date: date selector
		#    threshold: defines a threshold for visualizing different range of values.
		fluidRow(
			column(width=6, offset=2,
				   selectInput("date", "select date", choices = dates),
		  column(width=6, offset=2,
		         sliderInput("threshold", "threshold:", 
		                     min=1, max=500, value=100)
		  )
		)
		)
	),
	server = function(input, output) {
		output$map <- renderLeaflet({
		  df$val = df$count/input$threshold
		  df$val[df$val>1] = 1
		  dates = unique(df$date)
  	  leaflet(df[df$date == input$date,]) %>% addTiles %>% addCircleMarkers(lat = ~lat, lng = ~lng, radius = 2, color = ~rgb(val,0,0), stroke = FALSE, fillOpacity = ~(val^2))
		})
	}
)

