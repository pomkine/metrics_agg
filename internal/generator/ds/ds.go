package ds

import "math/rand"

type Config struct {
	ID            string
	InitValue     int
	MaxChangeStep int
}

type DataSource struct {
	c         Config
	nextValue int
}

func NewDataSource(c Config) *DataSource {
	return &DataSource{c: c, nextValue: c.InitValue}
}

func (s *DataSource) GetData() Data {
	v := s.nextValue
	s.nextValue += rand.Intn(s.c.MaxChangeStep)
	return Data{
		ID:    s.c.ID,
		Value: v,
	}
}

type Data struct {
	ID    string
	Value int
}
