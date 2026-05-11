package common

type Constant struct {
	JWT_SECRET string
}

func NewConstant() *Constant {
	return &Constant{
		JWT_SECRET: "vite-express-TOKEN",
	}
}
