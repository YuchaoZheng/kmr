package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/config"
	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/master"
	"github.com/naturali/kmr/util"
	"github.com/naturali/kmr/util/log"
	"github.com/naturali/kmr/worker"

	"github.com/urfave/cli"
	"k8s.io/client-go/rest"
)

func loadBuckets(m, i, r, f *config.BucketDescription) ([]bucket.Bucket, error) {
	mapBucket, err := bucket.NewBucket(m.BucketType, m.Config)
	if err != nil {
		return nil, err
	}
	interBucket, err := bucket.NewBucket(i.BucketType, i.Config)
	if err != nil {
		return nil, err
	}
	reduceBucket, err := bucket.NewBucket(r.BucketType, r.Config)
	if err != nil {
		return nil, err
	}
	flushBucket, err := bucket.NewBucket(f.BucketType, f.Config)
	if err != nil {
		return nil, err
	}

	return []bucket.Bucket{
		mapBucket, interBucket, reduceBucket, flushBucket,
	}, nil
}

func loadBucketsFromRemote(conf *config.RemoteConfig) ([]bucket.Bucket, error) {
	buckets, err := loadBuckets(conf.MapBucket, conf.InterBucket, conf.ReduceBucket, conf.FlushBucket)
	return buckets, err
}

func loadBucketsFromLocal(conf *config.LocalConfig) ([]bucket.Bucket, error) {
	buckets, err := loadBuckets(conf.MapBucket, conf.InterBucket, conf.ReduceBucket, conf.FlushBucket)
	return buckets, err
}

var userDefinedArgHandler func([]string)

func BindArgumentHandler(handler func([]string)) {
	userDefinedArgHandler = handler
}

// If we are in a docker container or in local
func IsInContainer() bool {
    if _, err := os.Stat("/.dockerenv"); os.IsNotExist(err) {
        return false;
    } else {
        return true;
    }
}

func Run(job *jobgraph.Job) {
	log.Debug("Call user defined argument handler")
	flag := false
	for idx, arg := range os.Args {
		if arg == "--" {
			log.Info("User arguments are", os.Args[idx+1:])
			if userDefinedArgHandler != nil {
				userDefinedArgHandler(os.Args[idx+1:])
			} else {
				log.Info("No user handler to handle arguments.")
			}
			flag = true
			os.Args = os.Args[:idx]
			break
		}
	}

	if !flag {
		log.Info("No user arguments found.")
		if userDefinedArgHandler != nil {
			userDefinedArgHandler([]string{})
		}
	}

	if len(job.GetName()) == 0 {
		job.SetName("anonymous-kmr-job")
	}

	var err error

	repMap := map[string]string{
		"${JOBNAME}": job.GetName(),
	}
	var conf *config.KMRConfig

	var buckets []bucket.Bucket
	var assetFolder string

	jobCommands := []cli.Flag{
		cli.IntFlag{
			Name:  "port",
			Value: 50051,
			Usage: "Define how many workers",
		},
		cli.IntFlag{
			Name:  "worker-num",
			Value: 1,
			Usage: "Define how many workers",
		},
		cli.IntFlag{
			Name:  "cpu-limit",
			Value: 2,
			Usage: "Define worker cpu limit when run in k8s",
		},
		cli.StringFlag{
			Name:  "check-point",
			Usage: "Assign a check point file, which should be in map bucket",
			Value: "checkpoint",
		},
		cli.BoolFlag{
			Name:  "fresh-run",
			Usage: "delete old check point and rerun",
		},
		cli.IntFlag{
			Name: "max-retries",
			Value: 0,
			Usage: "If a task fails more than MAX-RETRIES, it'll be regarded as succeeded. <=0 mean unlimited",
		},
	}

	app := cli.NewApp()
	app.Name = job.GetName()
	app.Description = "A KMR application named " + job.GetName()
	app.Author = "Naturali"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:  "config",
			Usage: "Load config from `FILES`",
		},
		cli.Int64Flag{
			Name:   "random-seed",
			Value:  time.Now().UnixNano(),
			Usage:  "Used to synchronize information between workers and master",
			Hidden: true,
		},
		cli.StringFlag{
			Name:  "asset-folder",
			Value: "./assets",
			Usage: "Files under asset folder will be packed into docker image. " +
				"KMR APP can use this files as they are under './'",
		},
	}
	app.Before = func(c *cli.Context) error {
		conf = config.LoadConfigFromMultiFiles(repMap, append(config.GetConfigLoadOrder(), c.StringSlice("config")...)...)
		var err error
		job.ValidateGraph()

		assetFolder, err = filepath.Abs(c.String("asset-folder"))
		if err != nil {
			return err
		}
		if f, err := os.Stat(assetFolder); err != nil || !f.IsDir() {
			if os.IsNotExist(err) {
				err := os.MkdirAll(assetFolder, 0777)
				if err != nil {
					return cli.NewMultiError(err)
				}
				originAfter := app.After
				app.After = func(ctx *cli.Context) (err error) {
					err = nil
					if originAfter != nil {
						err = originAfter(ctx)
					}
					os.RemoveAll(assetFolder)
					return
				}
			} else {
				return cli.NewExitError("Asset folder "+assetFolder+" is incorrect", 1)
			}
		}
		log.Info("Asset folder is", assetFolder)

		return nil
	}

	app.Commands = []cli.Command{
		{
			Name: "internal-print-config",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output",
					Value: "-",
				},
			},
			Hidden: true,
			Action: func(ctx *cli.Context) error {
				var output *os.File
				outFile := ctx.String("output")
				if outFile == "-" {
					output = os.Stdout
				} else {
					if output, err = os.Create(outFile); err != nil {
						return err
					}
				}
				configJson, err := json.MarshalIndent(conf, "", "\t")
				if err != nil {
					return err
				}
				output.WriteString(string(configJson) + "\n")
				output.Close()
				return nil
			},
		},
		{
			Name:   "internal-deploy",
			Hidden: true,
			Flags: append([]cli.Flag{
				cli.StringFlag{
					Name:  "binary",
					Usage: "kmr job's binary path in docker image",
				},
				cli.StringFlag{
					Name:  "config",
					Usage: "kmr job's config file path in docker image",
				},
				cli.StringFlag{
					Name:  "image-name",
					Usage: "kmr job's docker image name",
				},
				cli.StringFlag{
					Name:  "work-dir",
					Usage: "kmr job's docker work directory",
				},
			},
				jobCommands...),
			Action: func(ctx *cli.Context) error {
				imageName := ctx.String("image-name")
				commands := []string{ctx.String("binary"),
					"--config", strings.Join([]string(ctx.GlobalStringSlice("config")), ","),
					"master",
					"--remote", "--port", fmt.Sprint(ctx.Int("port")),
					"--worker-num", fmt.Sprint(ctx.Int("worker-num")),
					"--cpu-limit", fmt.Sprint(ctx.Int("cpu-limit")),
					"--max-retries", fmt.Sprint(ctx.Int("max-retries")),
					"--image-name", imageName,
					"--service-name", job.GetName(),
					"--check-point", ctx.String("check-point"),
				}
				if ctx.Bool("fresh-run") {
					commands = append(commands, "--fresh-run")
				}
				pod, service, err := util.CreateK8sKMRJob(job.GetName(),
					*conf.Remote.ServiceAccount,
					*conf.Remote.Namespace,
					*conf.Remote.PodDesc, imageName, ctx.String("work-dir"),
					commands,
					int32(ctx.Int("port")))

				if err != nil {
					return cli.NewMultiError(err)
				}
				log.Info("Pod: ", pod, "Service: ", service)
				return nil
			},
		},
		{
			Name: "master",
			Flags: append([]cli.Flag{
				cli.BoolFlag{
					Name:  "remote",
					Usage: "Run on kubernetes",
				},
				cli.BoolFlag{
					Name:  "listen-only",
					Usage: "Listen and waiting for workers",
				},
				cli.StringFlag{
					Name:   "image-name",
					Usage:  "Worker docker image name used in remote mode",
					Hidden: true,
				},
				cli.StringFlag{
					Name:   "service-name",
					Usage:  "k8s service name used in remote mode",
					Hidden: true,
				},
			}, jobCommands...),
			Action: func(ctx *cli.Context) error {
				var workerCtl worker.WorkerCtl
				seed := ctx.GlobalInt64("random-seed")
				if ctx.Bool("remote") {
					var err error
					buckets, err = loadBucketsFromRemote(conf.Remote)

					if err != nil {
						log.Fatal("error when load remote bucket", err)
					}

					var k8sconfig *rest.Config

					k8sSchema := os.Getenv("KUBERNETES_API_SCHEMA")
					if k8sSchema != "http" { // InCluster
						k8sconfig, err = rest.InClusterConfig()
						if err != nil {
							log.Fatalf("Can't get incluster config, %v", err)
						}
					} else { // For debug usage. > source dev_environment.sh
						host := os.Getenv("KUBERNETES_SERVICE_HOST")
						port := os.Getenv("KUBERNETES_SERVICE_PORT")

						k8sconfig = &rest.Config{
							Host: fmt.Sprintf("%s://%s", k8sSchema, net.JoinHostPort(host, port)),
						}
						token := os.Getenv("KUBERNETES_API_ACCOUNT_TOKEN")
						if len(token) > 0 {
							k8sconfig.BearerToken = token
						}
					}
					exe, err1 := os.Executable()
					exe, err2 := filepath.EvalSymlinks(exe)
					if err1 != nil || err2 != nil {
						exe = os.Args[0]
						log.Error("Cannot use os.Executable to determine executable path, use", exe, "instead")
					}
					workerCtl = worker.NewK8sWorkerCtl(&worker.K8sWorkerConfig{
						Name:         job.GetName(),
						CPULimit:     fmt.Sprint(ctx.Int("cpu-limit")),
						Namespace:    *conf.Remote.Namespace,
						K8sConfig:    *k8sconfig,
						WorkerNum:    ctx.Int("worker-num"),
						Volumes:      *conf.Remote.PodDesc.Volumes,
						VolumeMounts: *conf.Remote.PodDesc.VolumeMounts,
						RandomSeed:   seed,
						Image:        ctx.String("image-name"),
						Command: []string{
							exe,
							"--config", strings.Join([]string(ctx.GlobalStringSlice("config")), ","),
							"worker",
							"--master-addr", fmt.Sprintf("%v:%v", ctx.String("service-name"), ctx.Int("port")),
						},
					})
				} else {
					buckets, err = loadBucketsFromLocal(conf.Local)
					if err != nil {
						log.Fatal("error when load remote bucket", err)
					}

					workerCtl = worker.NewLocalWorkerCtl(job, ctx.Int("port"), 64, buckets)
					os.Chdir(assetFolder)
				}

				if ctx.Bool("listen-only") {
					workerCtl = nil
				}

				if workerCtl != nil {
					workerCtl.StartWorkers(ctx.Int("worker-num"))
				}

				if ctx.Bool("fresh-run") {
					buckets[2].Delete(ctx.String("check-point"))
				}
				ck, err := master.OpenCheckPoint(buckets[2], ctx.String("check-point"))
				if err != nil {
					ck = nil
					log.Error(err)
				}

				m := master.NewMaster(job, strconv.Itoa(ctx.Int("port")), buckets[0], buckets[1], buckets[2], ck, ctx.Int("max-retries"))

				m.Run()

				if workerCtl != nil {
					workerCtl.StopWorkers()
				}
				return nil
			},
		},
		{
			Name: "worker",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "master-addr",
					Value: "localhost:50051",
					Usage: "Master's address, format: \"HOSTNAME:PORT\"",
				},
				cli.BoolFlag{
					Name:  "local",
					Usage: "This worker will read local config",
				},
			},
			Usage: "Run in remote worker mode, this will read remote config items",
			Action: func(ctx *cli.Context) error {
				randomSeed := ctx.GlobalInt64("random-seed")
				rand.Seed(randomSeed)
				workerID := rand.Int63()

				if ctx.Bool("local") {
					buckets, err = loadBucketsFromLocal(conf.Local)
					os.Chdir(assetFolder)
				} else {
					buckets, err = loadBucketsFromRemote(conf.Remote)
				}
				w := executor.NewWorker(job, workerID, ctx.String("master-addr"), 64, buckets[0], buckets[1], buckets[2], buckets[3])
				w.Run()
				return nil
			},
		},
		{
			Name: "deploy",
			Flags: append([]cli.Flag{
				cli.StringSliceFlag{
					Name: "image-tags",
				},
			}, jobCommands...),
			Usage: "Deploy KMR Application in k8s",
			Action: func(ctx *cli.Context) error {
				dockerWorkDir := "/kmrapp"
				// collect files under current folder
				configFilePath := path.Join(assetFolder, "internal-used-config.json")
				executablePath := path.Join(assetFolder, "internal-used-executable")

				if _, err := os.Stat(configFilePath); err == nil {
					return cli.NewExitError(configFilePath+"should not exists", 1)
				}
				if _, err := os.Stat(executablePath); err == nil {
					return cli.NewExitError(executablePath+"should not exists", 1)
				}

				configJson, err := json.MarshalIndent(conf, "", "\t")
				if err != nil {
					return err
				}
				err = ioutil.WriteFile(configFilePath, configJson, 0666)
				defer os.Remove(configFilePath)
				if err != nil {
					return err
				}

				//TODO Use Ermine to pack executable and dynamic library
				exe, err1 := os.Executable()
				exe, err2 := filepath.EvalSymlinks(exe)
				if err1 != nil || err2 != nil {
					return cli.NewMultiError(errors.New("cannot locate executable"), err1, err2)
				}
				os.Link(exe, executablePath)
				defer os.Remove(executablePath)

				tags := ctx.StringSlice("image-tags")
				if len(tags) == 0 {
					tags = append(tags, strings.ToLower(job.GetName())+":"+"latest")
				}

				files, err := filepath.Glob(assetFolder + "/*")
				if err != nil {
					return err
				}

				fmt.Println(files)
				imageName, err := util.CreateDockerImage(assetFolder, *conf.Remote.DockerRegistry, tags, files, dockerWorkDir)
				if err != nil {
					return err
				}

				commands := []string{dockerWorkDir + "/internal-used-executable", "--config", dockerWorkDir + "/internal-used-config.json",
					"master",
					"--remote", "--port", fmt.Sprint(ctx.Int("port")),
					"--worker-num", fmt.Sprint(ctx.Int("worker-num")),
					"--cpu-limit", fmt.Sprint(ctx.Int("cpu-limit")),
					"--image-name", imageName,
					"--service-name", job.GetName(),
					"--check-point", ctx.String("check-point"),
					"--max-retries", fmt.Sprint(ctx.Int("max-retries")),
				}
				if ctx.Bool("fresh-run") {
					commands = append(commands, "--fresh-run")
				}
				pod, service, err := util.CreateK8sKMRJob(job.GetName(),
					*conf.Remote.ServiceAccount,
					*conf.Remote.Namespace,
					*conf.Remote.PodDesc, imageName, dockerWorkDir,
					commands,
					int32(ctx.Int("port")))

				if err != nil {
					return cli.NewMultiError(err)
				}
				log.Info("Pod: ", pod, "Service: ", service)
				return nil
			},
		},
	}

	app.Run(os.Args)
	os.Exit(0)
}
