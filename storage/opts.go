package storage

type Opts struct {
	Root              string
	PathTransformFunc PathTransformFunc
}

type Option func(opts *Opts)

func WithRoot(root string) Option {
	return func(opts *Opts) {
		opts.Root = root
	}
}

func WithPathTransformFunc(pathTransform PathTransformFunc) Option {
	return func(opts *Opts) {
		opts.PathTransformFunc = pathTransform
	}
}
