package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const version = "0.0.1"

type Driver struct {
	Mutex   sync.Mutex
	Mutexes map[string]*sync.Mutex
	Dir     string
}

type User struct {
	Name    string `json:"name"`
	Age     string `json:"age"`
	Contact string `json:"contact"`
	Company string `json:"company"`
	Address Address
}

type Address struct {
	Country string `json:"country"`
	State   string `json:"state"`
	City    string `json:"city"`
	Pincode string `json:"pincode"`
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Info().Msg(version)

	dir := "./"

	db, err := New(dir)
	if err != nil {
		log.Fatal().Err(err).Msg("Ошибка создания базы данных")
	}

	employees := Input()

	Write(db, employees)
	// Read(db, "user", "Stas", User{})
	// ReadAll(db, "user", User{})
	// Delete(db, "user", "")

}

func Input() []User {
	var user User
	scanner := bufio.NewScanner(os.Stdin)
	input := map[string]*string{
		"Ваше имя":          &user.Name,
		"Ваш возраст":       &user.Age,
		"Ваш контакт":       &user.Contact,
		"название компании": &user.Company,
		"страну проживания": &user.Address.Country,
		"область":           &user.Address.State,
		"город":             &user.Address.City,
		"индекс":            &user.Address.Pincode,
	}

	for key, value := range input {
		log.Info().Msg(key)
		scanner.Scan()
		*value = scanner.Text()
	}
	return []User{user}
}

func Write(db *Driver, employees []User) {
	for _, value := range employees {
		db.Write("user", value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: Address{
				Country: value.Address.Country,
				State:   value.Address.State,
				City:    value.Address.City,
				Pincode: value.Address.Pincode,
			},
		})
		log.Info().Msgf("Запись %v создана", value.Name)
	}
}

func Read(db *Driver, collection string, resources string, v interface{}) {
	if err := db.Read(collection, resources, User{}); err != nil {
		log.Error().Err(err).Msg("Ошибка чтения записи из базы данных")
	}
}

func ReadAll(db *Driver, collection string, v interface{}) {
	records, err := db.ReadAll(collection)
	if err != nil {
		log.Error().Err(err).Msg("Ошибка чтения записей из базы данных")
	}

	fmt.Println(records)
	// allUsers := []User{}

	for _, value := range records {
		user := User{}
		if err := json.Unmarshal([]byte(value), &user); err != nil {
			log.Error().Err(err).Msg("Ошибка чтения записей")
		}
		// allUsers = append(allUsers, user)
	}
}

func Delete(db *Driver, collection string, resources string) {
	if err := db.Delete(collection, resources); err != nil {
		log.Error().Err(err).Msg("Ошибка удаления записи из базы данных")
	}
}

func New(dir string) (*Driver, error) {
	dir = filepath.Clean(dir)

	driver := Driver{
		Dir:     dir,
		Mutexes: make(map[string]*sync.Mutex),
	}

	_, err := os.Stat(dir)
	if err != nil {
		log.Error().Err(err).Msgf("База данных %v уже создана", dir)
		return &driver, err
	}
	log.Info().Msgf("Создание базы данных `%v`", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

func stat(path string) (file os.FileInfo, err error) {
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return fi, err
}

func (driver *Driver) Write(collection, resources string, v interface{}) error {
	if collection == "" {
		log.Error().Msg("Коллекция пустая, нет сохраненных записей")
		return nil
	}
	if resources == "" {
		log.Error().Msg("Ресурс пустой, нет сохраненных записей")
		return nil
	}
	mutex := driver.GetOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(collection, driver.Dir)
	resPath := filepath.Join(dir, resources+".json")
	tmpPath := resPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Error().Err(err).Msg("Ошибка создания директории")
		return err
	}

	file, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		log.Error().Err(err).Msg("Ошибка преобразования в JSON")
		return err
	}

	file = append(file, byte('\n'))
	if err := os.WriteFile(tmpPath, file, 0644); err != nil {
		log.Error().Err(err).Msg("Ошибка записи в файл")
		return err
	}

	return os.Rename(tmpPath, resPath)
}

func (driver *Driver) Read(collection, resources string, v interface{}) error {
	if collection == "" {
		log.Error().Msg("Коллекция пустая, нет сохраненных записей")
		return nil
	}
	if resources == "" {
		log.Error().Msg("Ресурс пустой, нет сохраненных записей")
		return nil
	}

	path := (resources + ".json")
	dir := filepath.Join(driver.Dir, collection, path)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Error().Err(err).Msg("Запись не найдена")
		return err
	}

	file, err := os.ReadFile(dir)
	if err != nil {
		log.Error().Err(err).Msg("Ошибка чтения файла")
		return err
	}
	if err := json.Unmarshal(file, &v); err != nil {
		log.Error().Err(err).Msg("Ошибка преобразования в JSON")
		return err
	}
	fmt.Print(string(file))
	return nil
}

func (driver *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		log.Error().Msg("Коллекция пустая, нет сохраненных записей")
		return nil, nil
	}

	dir := filepath.Join(driver.Dir, collection)

	if _, err := stat(dir); err != nil {
		log.Error().Err(err).Msg("Файлы отсутствуют")
		return nil, err
	}

	files, _ := os.ReadDir(dir)

	var records []string

	for _, file := range files {
		res, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			log.Error().Err(err).Msg("Ошибка чтения файла")
			return nil, err
		}

		records = append(records, string(res))
	}
	return records, nil
}

func (driver *Driver) Delete(collection, resources string) error {
	path := filepath.Join(collection, resources)

	mutex := driver.GetOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(driver.Dir, path)

	switch file, err := stat(dir); {
	case file == nil, err != nil:
		log.Error().Err(err).Msgf("Файл %v не найден", path)
		return nil
	case file.Mode().IsDir():
		log.Info().Msgf("Коллекция %v удалена", dir)
		return os.RemoveAll(dir)
	case file.Mode().IsRegular():
		log.Info().Msgf("Файл %v удален", dir)
		return os.RemoveAll(dir + ".json")
	}
	return nil
}

func (driver *Driver) GetOrCreateMutex(collection string) *sync.Mutex {
	driver.Mutex.Lock()
	defer driver.Mutex.Unlock()

	m, ok := driver.Mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		driver.Mutexes[collection] = m
	}
	return m
}
