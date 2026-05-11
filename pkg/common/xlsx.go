package common

import (
	"bytes"
	"fmt"

	"github.com/xuri/excelize/v2"
)

const XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

type ExcelSheet struct {
	Name string
	Rows [][]interface{}
}

type Xlsx struct {
	Sheets []ExcelSheet
}

func NewXLSX(sheets *[]ExcelSheet) *Xlsx {
	return &Xlsx{
		Sheets: *sheets,
	}
}

func (xlsx Xlsx) Render() (*bytes.Buffer, error) {

	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	for _, sheet := range xlsx.Sheets {
		for idx, row := range sheet.Rows {
			cell, err := excelize.CoordinatesToCellName(1, idx+1)
			if err != nil {
				fmt.Println(err)
				return nil, nil
			}
			f.SetSheetRow(sheet.Name, cell, &row)
		}
	}
	return f.WriteToBuffer()
}
