from django.http import HttpResponse
from django.template import loader
from django.shortcuts import render,request,redirect

def add():
    print('hello')


def home(request):
    request.session['flag'] = 0
    template = loader.get_template("home.html")
    context = {'flag' : request.session['flag']}
    return HttpResponse(template.render(context,request))

# Create your views here.
