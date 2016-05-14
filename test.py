from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

def mapTut():

    m = Basemap(projection='mill',llcrnrlat=37,urcrnrlat=37.9,\
                llcrnrlon=-122.7,urcrnrlon=-122.1,resolution='c')
    m.drawcoastlines()
    m.drawcountries()
    m.drawstates()
    m.fillcontinents(color='#04BAE3',lake_color='#FFFFFF')
    m.drawmapboundary(fill_color='#FFFFFF')


    # Houston, Texas

    lat,lon = 37.7749,-122.4194
    x,y = m(lon,lat)
    m.plot(x,y, 'go')
    
    plt.title("Geo Plotting")
    plt.show()


mapTut()