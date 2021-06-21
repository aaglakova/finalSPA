package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/aaglakova/finalSPA/internal/validator"
	"time"
)

type Books struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"-"`
	Title     string    `json:"title"`
	Year      int32     `json:"year,omitempty"`
	Pages     Pages     `json:"pages,omitempty"`
	Version   int32     `json:"version"`
}

func ValidateBooks(v *validator.Validator, books *Books) {
	v.Check(books.Title != "", "title", "must be provided")
	v.Check(len(books.Title) <= 500, "title", "must not be more than 500 bytes long")

	v.Check(books.Year != 0, "year", "must be provided")
	v.Check(books.Year >= 1888, "year", "must be greater than 1888")
	v.Check(books.Year <= int32(time.Now().Year()), "year", "must not be in the future")

	v.Check(books.Pages != 0, "pages", "must be provided")
	v.Check(books.Pages > 0, "pages", "must be a positive integer")

}

type BooksModel struct {
	DB *sql.DB
}

// Add a placeholder method for inserting a new record in the movies table.
func (m BooksModel) Insert(books *Books) error {
	query := `INSERT INTO books (title, year, pages)
			VALUES ($1, $2, $3)
			RETURNING id, created_at, version`

	args := []interface{}{books.Title, books.Year, books.Pages}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return m.DB.QueryRowContext(ctx, query, args...).Scan(&books.ID, &books.CreatedAt, &books.Version)
}

// Add a placeholder method for fetching a specific record from the movies table.
func (m BooksModel) Get(id int64) (*Books, error) {
	if id < 1 {
		return nil, ErrRecordNotFound
	}
	query := `SELECT id, created_at, title, year, pages, version
			FROM comics
			WHERE id = $1`

	var books Books
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, id).Scan(
		&books.ID,
		&books.CreatedAt,
		&books.Title,
		&books.Year,
		&books.Pages,
		&books.Version,
	)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrRecordNotFound
		default:
			return nil, err
		}
	}
	return &books, nil
}

// Add a placeholder method for updating a specific record in the movies table.
func (m BooksModel) Update(books *Books) error {
	query := `UPDATE books
			SET title = $1, year = $2, pages = $3, version = version + 1
			WHERE id = $4 AND version = $5
			RETURNING version`
	args := []interface{}{
		books.Title,
		books.Year,
		books.Pages,
		books.ID,
		books.Version,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.DB.QueryRowContext(ctx, query, args...).Scan(&books.Version)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrEditConflict
		default:
			return err
		}
	}
	return nil
}

// Add a placeholder method for deleting a specific record from the movies table.
func (m BooksModel) Delete(id int64) error {
	if id < 1 {
		return ErrRecordNotFound
	}
	query := `DELETE FROM books
			WHERE id = $1`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := m.DB.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrRecordNotFound
	}
	return nil
}

func (m BooksModel) GetAll(title string, year int, filters Filters) ([]*Books, Metadata, error) {
	query := fmt.Sprintf(
		`
		SELECT count(*) OVER(), id, created_at, title, year, pages, version
		FROM books
		WHERE (to_tsvector('simple', title) @@ plainto_tsquery('simple', $1) OR $1 = '')
		AND (year = $2 OR $2 = -1)
		ORDER BY %s %s, id ASC
		LIMIT $3 OFFSET $4`, filters.sortColumn(), filters.sortDirection())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	args := []interface{}{title, year, filters.limit(), filters.offset()}

	rows, err := m.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, Metadata{}, err
	}
	defer rows.Close()

	totalRecords := 0
	books := []*Books{}
	for rows.Next() {
		var books Books
		err := rows.Scan(
			&totalRecords,
			&books.ID,
			&books.CreatedAt,
			&books.Title,
			&books.Year,
			&books.Pages,
			&books.Version,
		)
		if err != nil {
			return nil, Metadata{}, err
		}
		books = append(books, &books)
	}
	if err = rows.Err(); err != nil {
		return nil, Metadata{}, err
	}

	metadata := calculateMetadata(totalRecords, filters.Page, filters.PageSize)
	return books, metadata, nil
}
