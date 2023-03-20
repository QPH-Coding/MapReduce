module mrapps

go 1.19

require mr v0.0.0  // 这个会在你执行 go build 之后自动在该文件添加
replace mr => ../mr  // 指定需要的包目录去后面这个路径中寻找