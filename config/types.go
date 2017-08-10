package config

type PodDescription struct {
	Volumes      *[]map[string]interface{} `json:"volumes,omitempty" patchStrategy:"merge"`
	VolumeMounts *[]map[string]interface{} `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath"`
}

// BucketConfig Parameters of bucket
type BucketConfig map[string]interface{}

// BucketDescription BucketDescription
type BucketDescription struct {
	BucketType string       `json:"bucketType"`
	Config     BucketConfig `json:"config"`
}

type LocalConfig struct {
	MapBucket    *BucketDescription `json:"mapBucket"`
	InterBucket  *BucketDescription `json:"interBucket"`
	ReduceBucket *BucketDescription `json:"reduceBucket"`
}

type RemoteConfig struct {
	MapBucket    *BucketDescription `json:"mapBucket"`
	InterBucket  *BucketDescription `json:"interBucket"`
	ReduceBucket *BucketDescription `json:"reduceBucket"`
	PodDesc      *PodDescription    `json:"podDescription"`

	DockerRegistry *string `json:"dockerRegistry"`
	Namespace      *string `json:"namespace"`
	ServiceAccount *string `json:"serviceAccount"`
}

type KMRConfig struct {
	Local  *LocalConfig  `json:"local"`
	Remote *RemoteConfig `json:"remote"`
}

