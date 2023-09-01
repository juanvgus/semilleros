from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="my_app")

def test_upz():
  assert geolocator.reverse((4.646835	, -74.101619)).raw['address']['neighbourhood'] == 'Salitre Oriental'

def test_localidad():
  assert geolocator.reverse((4.646835	, -74.101619)).raw['address']['suburb'] == 'Localidad Teusaquillo'
