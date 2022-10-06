from prefect import flow,task

import httpx

@task
def fetch_weather(lat : float, lang:float ) :
	url = "https://api.open-meteo.com/v1/forecast/"
	weather= httpx.get(url,params=dict(latitude = lat, longitude = lang,hourly="temperature_2m"))
	most_recent_weather = float(weather.json()['hourly']['temperature_2m'][0])
	print(f"Most recent weather is {most_recent_weather}")
	return weather

@task
def save_weather(weather : float):
	with open("weather.csv","w+") as w:
		w.write(str(weather))
	return "Success"

@flow
def pipeline():
    temp = fetch_weather(38.9,-77.0)
    save_weather(temp)

if __name__ =="__main__":
	pipeline()