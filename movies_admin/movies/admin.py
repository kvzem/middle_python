from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    list_filter = ('name',)
    search_fields = ('name', 'description',)
    pass


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre', 'film_work',)


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ('person', 'film_work',)


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmWorkInline,)
    list_display = ('title', 'type', 'creation_date', 'rating',)
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    list_filter = ('full_name',)
    search_fields = ('full_name',)
