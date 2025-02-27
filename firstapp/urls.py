from django.urls import path
from . import views

urlpatterns = [
     path('', views.home, name='home'),
     path('signup.html',views.signup,name='signup'),
     path('login',views.login,name='login'),
     path('villagedashboard',views.village_dashboard,name='villagedashboard'),
     path('citizens',views.citizens,name='citizens'),
     path('panchayat_employees',views.panemp,name='panchayat_employees'),
     
]
