from django.urls import path
from . import views

urlpatterns = [
     path('', views.home, name='home'),
     path('signup.html',views.signup,name='signup'),
     path('login',views.login,name='login'),
     path('villagedashboard',views.village_dashboard,name='villagedashboard'),
     path('citizens',views.citizens,name='citizens'),
     path('citizens/tax_payments',views.citizenTaxes,name = 'citizenTaxes'),
     path('citizens/mycertificates',views.mycertificates,name = 'mycertificates'),
     path('panchayat_employees',views.panemp,name='panchayat_employees'),
     path('citizens/paymentPage/',views.citizenPayments,name='citizenPayments'),
     path('citizens/previousTransactions/',views.previousTransactions,name='previousTransactions'),
     path('citizens/land_records', views.land_records, name = 'land_records'),

     
]
