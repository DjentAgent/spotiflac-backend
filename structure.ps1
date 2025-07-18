# Создаём папки
$dirs = @(
  'src\spotiflac_backend',
  'src\spotiflac_backend\api',
  'src\spotiflac_backend\api\v1',
  'src\spotiflac_backend\core',
  'src\spotiflac_backend\services',
  'src\spotiflac_backend\tasks',
  'src\spotiflac_backend\models'
)
foreach ($d in $dirs) {
  New-Item -Path $d -ItemType Directory -Force | Out-Null
}

# Создаём файлы
$files = @(
  'src\spotiflac_backend\__init__.py',
  'src\spotiflac_backend\main.py',
  'src\spotiflac_backend\api\__init__.py',
  'src\spotiflac_backend\api\v1\__init__.py',
  'src\spotiflac_backend\api\v1\health.py',
  'src\spotiflac_backend\core\__init__.py',
  'src\spotiflac_backend\core\config.py',
  'src\spotiflac_backend\services\__init__.py',
  'src\spotiflac_backend\services\rutracker.py',
  'src\spotiflac_backend\tasks\__init__.py',
  'src\spotiflac_backend\tasks\torrent_tasks.py',
  'src\spotiflac_backend\models\__init__.py',
  'src\spotiflac_backend\models\track.py'
)
foreach ($f in $files) {
  New-Item -Path $f -ItemType File -Force | Out-Null
}

Write-Host "Структура папок и файлов создана!"
