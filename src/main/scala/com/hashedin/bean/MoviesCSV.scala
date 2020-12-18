package com.hashedin.bean

case class MoviesCSV(title: String, original_title: String, year: Int, date_published: String, genre: String,
                     duration: Int, country: String, language: String, director: String, writer: String,
                     production_company: String, actors: String, description: String, avg_vote: Double,
                     votes: Int, budget: String, usa_gross_income: String, worlwide_gross_income: String,
                     metascore: Int, reviews_from_users: Int, reviews_from_critics: Int, imdb_title_id: String)
