from flask import Flask, render_template, request, jsonify, session
from gevent.pywsgi import WSGIServer
from werkzeug.middleware.proxy_fix import ProxyFix
import threading
import uuid
import json
import subprocess
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
from werkzeug.utils import secure_filename
import logging
from logging.handlers import RotatingFileHandler
import shutil
import time
import signal
import sys
import re
import traceback
import requests
from urllib.parse import urlparse
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.exceptions import NotFound

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config["APPLICATION_ROOT"] = "/sar-service"
app.config['PREFERRED_URL_SCHEME'] = 'https'  # URL 생성 시 HTTPS 사용
app.config['SESSION_COOKIE_SECURE'] = True  # 이미 있음
app.config['SESSION_COOKIE_HTTPONLY'] = True  # 이미 있음
app.config['SESSION_COOKIE_PATH'] = '/sar-service'  # 쿠키 경로 설정

# 로깅 설정
LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)

# 콘솔 핸들러 추가
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# 파일 핸들러 설정
file_handler = RotatingFileHandler(
    LOG_DIR / 'app.log',
    maxBytes=10485760,  # 10MB
    backupCount=5
)
file_handler.setLevel(logging.DEBUG)

# 더 상세한 로그 포맷 설정
log_format = logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - '
    'ProcessID:%(process)d - ThreadID:%(thread)d - '
    '%(funcName)s - %(message)s'
)

console_handler.setFormatter(log_format)
file_handler.setFormatter(log_format)

# 루트 로거 설정
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # 전체 로깅 레벨 DEBUG로 설정
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)

# 특정 모듈의 로그 레벨 조정 (선택적)
logging.getLogger('werkzeug').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.info("로깅 시스템이 초기화되었습니다.")

# 업로드 디렉토리 설정
UPLOAD_ROOT = Path('uploads')
UPLOAD_ROOT.mkdir(exist_ok=True)

# 세션 및 파일 처리 상태 저장
processing_status = {}
processing_lock = threading.Lock()

# 앱 설정 추가
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)  # 세션 유효 기간

def cleanup_old_files():
    """24시간 이상 된 파일 및 빈 디렉토리 정리"""
    try:
        current_time = datetime.now()
        for session_dir in UPLOAD_ROOT.iterdir():
            if session_dir.is_dir():
                # 디렉토리 내 파일 검사
                for file_path in session_dir.iterdir():
                    if file_path.is_file():
                        file_age = current_time - datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_age > timedelta(hours=24):
                            file_path.unlink()
                            logger.info(f"Deleted old file: {file_path}")

                # 빈 디렉토리 삭제
                if not any(session_dir.iterdir()):
                    session_dir.rmdir()
                    logger.info(f"Removed empty directory: {session_dir}")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

@app.route('/')
def index():
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file selected'}), 400

        file = request.files['file']
        time_filter_enabled = request.form.get('timeFilterEnabled') == 'true'
        start_time = request.form.get('startTime', '')
        end_time = request.form.get('endTime', '')

        logger.info("=== Upload Request ===")
        logger.info(f"Time filter enabled: {time_filter_enabled}")
        logger.info(f"Start time: {start_time}")
        logger.info(f"End time: {end_time}")

        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        # 세션 ID 확인 및 생성
        session_id = session.get('session_id')
        if not session_id:
            session_id = str(uuid.uuid4())
            session['session_id'] = session_id

        # 파일 저장 및 처리
        filename = secure_filename(file.filename)
        session_dir = UPLOAD_ROOT / session_id
        session_dir.mkdir(exist_ok=True)
        file_path = session_dir / filename
        file.save(str(file_path))

        # 시간 필터 정보를 처리 함수에 전달
        processing_thread = threading.Thread(
            target=process_sar_data_async,
            args=(str(file_path), session_id, time_filter_enabled, start_time, end_time)
        )
        processing_thread.daemon = True
        processing_thread.start()

        with processing_lock:
            processing_status[session_id] = {'status': 'processing'}

        return jsonify({'session_id': session_id, 'status': 'processing'})

    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        return jsonify({'error': 'File upload failed'}), 500

@app.route('/process-url', methods=['POST'])
def process_url():
    try:
        file_url = request.form.get('file_url')
        if not file_url:
            return jsonify({'error': 'URL이 제공되지 않았습니다.'}), 400

        time_filter_enabled = request.form.get('timeFilterEnabled') == 'true'
        start_time = request.form.get('startTime', '')
        end_time = request.form.get('endTime', '')

        # 세션 ID 확인 및 생성
        session_id = session.get('session_id')
        if not session_id:
            session_id = str(uuid.uuid4())
            session['session_id'] = session_id

        # URL에서 파일명 추출
        parsed_url = urlparse(file_url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            filename = f"downloaded_sar_{int(time.time())}"

        # 세션 디렉토리 생성
        session_dir = UPLOAD_ROOT / session_id
        session_dir.mkdir(exist_ok=True)
        file_path = session_dir / secure_filename(filename)

        # URL에서 파일 다운로드
        response = requests.get(file_url, stream=True)
        response.raise_for_status()

        # 파일 저장
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # 파일 처리 시작
        if not is_sar_binary(str(file_path)):
            raise Exception("올바른 SAR 바이너리 파일이 아닙니다.")

        processing_thread = threading.Thread(
            target=process_sar_data_async,
            args=(str(file_path), session_id, time_filter_enabled, start_time, end_time)
        )
        processing_thread.daemon = True
        processing_thread.start()

        with processing_lock:
            processing_status[session_id] = {'status': 'processing'}

        return jsonify({'session_id': session_id, 'status': 'processing'})

    except requests.exceptions.RequestException as e:
        logger.error(f"URL 다운로드 오류: {str(e)}")
        return jsonify({'error': f'파일 다운로드 실패: {str(e)}'}), 500
    except Exception as e:
        logger.error(f"URL 처리 오류: {str(e)}")
        return jsonify({'error': f'URL 처리 중 오류 발생: {str(e)}'}), 500

def is_sar_binary(file_path):
    """SA 바이너리 파일 여부 확인"""
    try:
        # 파일 크기 확인 (최소 크기)
        if os.path.getsize(file_path) < 1024:  # 1KB 미만이면 의심
            raise Exception("유효하지 않은 SAR 파일입니다.")

        # 파일 내용 확인
        with open(file_path, 'rb') as f:
            # 처음 몇 바이트를 읽어서 확인
            header = f.read(1024)

        # 텍스트 파일 여부 확인
        try:
            header_text = header.decode('ascii')
            # 텍스트로 디코딩이 가능하고 일반적인 텍스트 파일 패턴이 있다면
            if any(pattern in header_text.lower() for pattern in ['linux', 'cpu', 'memory', 'text']):
                raise Exception("텍스트 파일입니다. SAR 바이너리 파일을 업로드해주세요.")
        except UnicodeDecodeError:
            # 바이너리 파일인 경우 (디코딩 실패)
            # SAR 바이너리 파일의 특징적인 패턴 확인
            # 예: 매직 넘버나 특정 바이트 시퀀스 확인
            if not any(pattern in header for pattern in [b'\x00\x00\x00', b'\xff\xff\xff']):
                raise Exception("올바른 SAR 바이너리 파일이 아닙니다.")
            return True

        return False

    except Exception as e:
        logger.error(f"Error checking file type: {str(e)}")
        raise

def process_sar_data_async(file_path, session_id, time_filter_enabled=False, start_time='', end_time=''):
    try:
        logger.info(f"Starting async processing for session: {session_id}")

        # SAR 데이터 처리
        result = process_sar_data(file_path, time_filter_enabled, start_time, end_time)

        with processing_lock:
            processing_status[session_id] = {
                'status': 'completed',
                'result': result
            }
        logger.info(f"Processing completed for session: {session_id}")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Processing error for session {session_id}: {error_msg}")
        logger.error(traceback.format_exc())
        with processing_lock:
            processing_status[session_id] = {
                'status': 'error',
                'error': error_msg
            }
    finally:
        try:
            # 임시 파일 삭제
            os.remove(file_path)
            logger.info(f"Temporary file removed: {file_path}")
        except Exception as e:
            logger.error(f"Error removing temporary file: {str(e)}")

@app.route('/status/<session_id>')
def check_status(session_id):
    try:
        logger.info(f"Checking status for session: {session_id}")

        with processing_lock:
            status_info = processing_status.get(session_id)

        if not status_info:
            logger.error(f"No status found for session: {session_id}")
            return jsonify({'status': 'error', 'error': 'Session not found'}), 404

        logger.debug(f"Status info: {json.dumps(status_info, default=str)}")

        # 결과 데이터가 있는 경우 timestamp 확인
        if status_info.get('status') == 'completed' and 'result' in status_info:
            result = status_info['result']
            if not result or not isinstance(result, dict):
                logger.error(f"Invalid result data structure: {result}")
                return jsonify({'status': 'error', 'error': 'Invalid result data'}), 500

            # 기본 데이터 구조 확인
            required_fields = ['cpu', 'memory', 'network', 'disk', 'load']
            for field in required_fields:
                if field not in result:
                    logger.error(f"Missing required field: {field}")
                    return jsonify({'status': 'error', 'error': f'Missing {field} data'}), 500

        return jsonify(status_info)

    except Exception as e:
        logger.error(f"Error checking status: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': f'Status check failed: {str(e)}'
        }), 500

# 파일 정리 스케줄러 시작
def start_cleanup_scheduler():
    while True:
        cleanup_old_files()
        # 6시간마다 실행
        time.sleep(21600)

def get_kernel_version(file_path):
    """SA 파일의 헤더에서 커널 버전 확인"""
    try:
        with open(file_path, 'rb') as f:
            header = f.read(1024).decode('utf-8', errors='ignore')

        logger.debug(f"SA file header content: {header[:200]}")

        # 9.x 버전 체크 (커널 5.14)
        if any(pattern in header for pattern in ['5.14', 'RHEL9', 'Rocky9']):
            if 'Rocky' in header:
                logger.info("Detected Rocky 9.x")
                return '9.x', 'Rocky'
            else:
                logger.info("Detected RHEL/CentOS 9.x")
                return '9.x', 'RHEL/CentOS'
        # 8.x 버전 체크
        elif any(pattern in header for pattern in ['4.18', 'RHEL8', 'Rocky8', 'CentOS8']):
            if 'Rocky' in header:
                logger.info("Detected Rocky 8.x")
                return '8.x', 'Rocky'
            else:
                logger.info("Detected RHEL/CentOS 8.x")
                return '8.x', 'RHEL/CentOS'
        # 7.x 버전 체크
        elif any(pattern in header for pattern in ['3.10', 'RHEL7', 'CentOS7']):
            logger.info("Detected RHEL/CentOS 7.x")
            return '7.x', 'RHEL/CentOS'
        else:
            logger.error(f"Unable to determine version. Header content: {header[:200]}")
            raise Exception("Unsupported kernel version")

    except Exception as e:
        logger.error(f"Error checking kernel version: {str(e)}")
        raise

def get_sadf_command(kernel_version):
    """커널 버전에 따른 sadf 명령어 경로 반환"""
    import os
    import subprocess
    import logging

    if kernel_version == '9.x':
        sadf_path = '/opt/gwsar/bin/sadf9'
    elif kernel_version == '8.x':
        sadf_path = '/opt/gwsar/bin/sadf8'
    elif kernel_version == '7.x':
        sadf_path = '/opt/gwsar/bin/sadf7'
    else:
        raise Exception("Unsupported kernel version")

    # 파일 존재 확인 및 상세 로깅
    logger.info(f"Checking sadf path for kernel {kernel_version}: {sadf_path}")
    if os.path.exists(sadf_path):
        logger.info(f"File exists: {sadf_path}")
        logger.info(f"File size: {os.path.getsize(sadf_path)} bytes")
        logger.info(f"File permissions: {oct(os.stat(sadf_path).st_mode)}")

        # 공유 라이브러리 의존성 확인
        try:
            ldd_output = subprocess.run(['ldd', sadf_path], capture_output=True, text=True)
            logger.info(f"Shared library dependencies:\n{ldd_output.stdout}")
        except Exception as e:
            logger.warning(f"Failed to check library dependencies: {str(e)}")

        # 파일 실행 테스트
        try:
            test_output = subprocess.run([sadf_path, '-V'], capture_output=True, text=True)
            logger.info(f"Test execution output:\n{test_output.stdout}")
        except Exception as e:
            logger.error(f"Test execution failed: {str(e)}")

        return sadf_path
    else:
        logger.error(f"sadf command not found: {sadf_path}")

        # 대체 경로 시도
        system_sadf = '/usr/bin/sadf'
        if os.path.exists(system_sadf):
            logger.info(f"Using system sadf instead: {system_sadf}")
            return system_sadf

        raise Exception(f"sadf command not found: {sadf_path}")

def process_disk_metrics(disk, kernel_version):
    """커널 버전별 디스크 메트릭 계산"""
    try:
        logger.debug(f"Processing disk metrics for {disk['disk-device']} ({kernel_version})")

        # 9.x 버전 처리 추가
        if kernel_version == '9.x':
            read_mb = float(disk.get('rkB', 0)) / 1024    # KB → MB
            write_mb = float(disk.get('wkB', 0)) / 1024   # KB → MB
            util_percent = float(disk.get('util-percent', 0))
        elif kernel_version == '7.x':
            rd_sec = float(disk['rd_sec'])
            wr_sec = float(disk['wr_sec'])
            read_mb = (rd_sec * 512) / (1024 * 1024)
            write_mb = (wr_sec * 512) / (1024 * 1024)
            util_percent = float(disk.get('util-percent', 0))
        else:  # 8.x
            read_mb = float(disk.get('rkB', 0)) / 1024
            write_mb = float(disk.get('wkB', 0)) / 1024
            util_percent = float(disk.get('util-percent', 0))

        metrics = {
            'read_mb': read_mb,
            'write_mb': write_mb,
            'tps': float(disk['tps']),
            'areq_sz': float(disk.get('areq-sz', disk.get('avgrq-sz', 0))),
            'avgqu_sz': float(disk.get('aqu-sz', disk.get('avgqu-sz', 0))),
            'await': float(disk.get('await', 0)),
            'svctm': float(disk.get('svctm', 0)) if 'svctm' in disk else 0,
            'util_percent': util_percent
        }

        logger.debug(f"Calculated metrics: {json.dumps(metrics, indent=2)}")
        return metrics

    except Exception as e:
        logger.error(f"Error processing disk metrics: {str(e)}")
        logger.error(f"Raw disk data: {json.dumps(disk, indent=2)}")
        raise

def get_sar_metadata(file_path, env):
    """SAR 명령어 출력에서 메타데이터 추출"""
    try:
        # 커널 버전 확인
        kernel_version, os_type = get_kernel_version(file_path)
        logger.info(f"Detected kernel version: {kernel_version}")

        # 버전별 명령어 선택
        if kernel_version == '7.x':
            cmd = ['/opt/gwsar/bin/sadf7', '-j', str(file_path), '--', '-n', 'DEV']
        elif kernel_version == '8.x':
            cmd = ['/opt/gwsar/bin/sadf8', '-j', str(file_path), '--', '-n', 'DEV']
        elif kernel_version == '9.x':
            cmd = ['/opt/gwsar/bin/sadf9', '-j', str(file_path), '--', '-n', 'DEV']
        else:
            raise Exception(f"Unsupported kernel version: {kernel_version}")

        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)

        # JSON 파싱
        try:
            json_data = json.loads(result.stdout)
            logger.info("Successfully parsed JSON output")

            if 'sysstat' in json_data and 'hosts' in json_data['sysstat']:
                host_info = json_data['sysstat']['hosts'][0]

                # 버전별 호스트명 추출
                hostname = "Unknown"
                if kernel_version == '7.x':
                    hostname = host_info.get('nodename', host_info.get('hostname', 'Unknown'))
                elif kernel_version == '8.x':
                    hostname = host_info.get('hostname', host_info.get('nodename', 'Unknown'))
                elif kernel_version == '9.x':
                    hostname = host_info.get('nodename', host_info.get('hostname', 'Unknown'))

                logger.info(f"Extracted hostname: {hostname}")

                # 버전별 날짜 추출
                sar_date = "Unknown"
                if kernel_version in ['7.x', '8.x', '9.x']:
                    file_date = None

                    # 날짜 필드 확인 순서
                    date_fields = ['file-date', 'date', 'timestamp']
                    for field in date_fields:
                        if field in host_info:
                            file_date = host_info[field]
                            break

                    if file_date:
                        try:
                            # 타임스탬프 처리
                            if isinstance(file_date, (int, float)):
                                date_obj = datetime.fromtimestamp(float(file_date))
                                sar_date = date_obj.strftime('%m/%d/%Y')
                            else:
                                # 다양한 날짜 형식 처리
                                date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']
                                for date_format in date_formats:
                                    try:
                                        date_obj = datetime.strptime(str(file_date), date_format)
                                        sar_date = date_obj.strftime('%m/%d/%Y')
                                        break
                                    except ValueError:
                                        continue
                        except Exception as e:
                            logger.error(f"Date parsing error: {str(e)}")
                            sar_date = "Unknown"

                logger.info(f"Extracted date: {sar_date}")

                # 버전별 OS 버전 확인
                os_version = "Unknown"
                kernel_info = host_info.get('release', host_info.get('kernel', ''))

                if kernel_info:
                    if '3.10.0' in kernel_info and 'el7' in kernel_info:
                        os_version = "RHEL/CentOS 7.x"
                    elif '4.18.0' in kernel_info and 'el8' in kernel_info:
                        os_version = "RHEL/CentOS/Rocky 8.x"
                    elif '5.14.0' in kernel_info and 'el9' in kernel_info:
                        os_version = "RHEL/Rocky 9.x"

                logger.info(f"Detected OS version: {os_version}")

                # CPU 코어 수 추출
                cpu_fields = ['number-of-cpus', 'cpu-count', 'cpu-cores']
                cpu_count = "Unknown"
                for field in cpu_fields:
                    if field in host_info:
                        cpu_count = host_info[field]
                        break

                metadata = {
                    'hostname': hostname,
                    'date': sar_date,
                    'os_version': os_version,
                    'kernel_version': kernel_info,
                    'cpu_count': cpu_count
                }

                logger.info("=== Extracted Metadata ===")
                logger.info(f"Hostname: {metadata['hostname']}")
                logger.info(f"SAR Date: {metadata['date']}")
                logger.info(f"OS Version: {metadata['os_version']}")
                logger.info(f"Kernel Version: {metadata['kernel_version']}")
                logger.info(f"CPU Count: {metadata['cpu_count']}")

                return metadata

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {str(e)}")
            raise Exception("SAR 파일 파싱 실패")

    except Exception as e:
        logger.error(f"Error extracting metadata: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'hostname': "Unknown",
            'date': "Unknown",
            'os_version': "Unknown",
            'kernel_version': "Unknown",
            'cpu_count': "Unknown"
        }

def process_sar_data(file_path, time_filter_enabled=False, start_time='', end_time=''):
    """SAR 데이터 처리"""
    try:
        logger.info("=== Processing SAR Data ===")
        logger.info(f"Time filter: {time_filter_enabled}")
        logger.info(f"Time range: {start_time} - {end_time}")

        # 환경변수 설정
        env = os.environ.copy()
        env['LANG'] = 'C'

        # 메타데이터 추출 (SAR 명령어 출력 사용)
        metadata = get_sar_metadata(file_path, env)
        logger.info(f"SAR file metadata: {metadata}")

        # 커널 버전 확인
        kernel_version, os_type = get_kernel_version(file_path)
        logger.info(f"Processing SAR data for kernel {kernel_version}")

        # 버전별 sadf 명령어 선택
        sadf_cmd = get_sadf_command(kernel_version)
        logger.info(f"Using sadf command: {sadf_cmd}")

        # 기본 명령어 구성
        base_cmd = [sadf_cmd, '-j', str(file_path), '--']
        if time_filter_enabled and start_time and end_time:
            logger.info(f"Applying time filter: {start_time} - {end_time}")
            base_cmd.extend(['-s', start_time, '-e', end_time])

        logger.info(f"Final command: {' '.join(base_cmd)}")

        # 각 메트릭 수집 (버전별 명령어 구성)
        metrics = {
            'cpu': base_cmd + ['-u', 'ALL'],
            'memory': base_cmd + ['-r'],
            'network': base_cmd + ['-n', 'DEV'],
            'disk': base_cmd + ['-d'],
            'load': base_cmd + ['-q']
        }

        data = {
            'timestamp': [],
            'cpu': {'user': [], 'system': [], 'iowait': [], 'idle': []},
            'memory': {'metrics': {'used': [], 'cache': [], 'buffer': [], 'commit': []}},
            'network': {'metrics': {'interfaces': [], 'rx_bytes': {}, 'tx_bytes': {}}},
            'disk': {'metrics': {'devices': [], 'read_mb': {}, 'write_mb': {}, 'util_percent': {}}},
            'load': {'metrics': {'ldavg_1': [], 'ldavg_5': [], 'ldavg_15': [], 'plist_sz': []}}
        }

        for metric_name, cmd in metrics.items():
            logger.info(f"=== {metric_name.upper()} Command ===")
            logger.info(f"Command: {' '.join(cmd)}")

            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)
                logger.info(f"Output sample:\n{result.stdout[:500]}")

                # JSON 데이터 파싱
                json_data = json.loads(result.stdout)

                # 데이터 처리
                for item in json_data['sysstat']['hosts'][0]['statistics']:
                    ts = item.get('timestamp', {})
                    if isinstance(ts, dict):
                        utc_time = datetime.strptime(f"{ts.get('date')} {ts.get('time')}", '%Y-%m-%d %H:%M:%S')
                        kst_time = utc_time.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=9)))
                        timestamp = kst_time.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        utc_time = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                        kst_time = utc_time.astimezone(timezone(timedelta(hours=9)))
                        timestamp = kst_time.strftime('%Y-%m-%d %H:%M:%S')

                    if timestamp not in data['timestamp']:
                        data['timestamp'].append(timestamp)

                    if metric_name == 'cpu':
                        cpu_stats = None
                        if 'cpu-load' in item:
                            for cpu in item['cpu-load']:
                                if cpu.get('cpu') == 'all':
                                    cpu_stats = cpu
                                    break
                        elif 'cpu-load-all' in item and item['cpu-load-all']:
                            cpu_stats = item['cpu-load-all'][0]

                        if cpu_stats:
                            data['cpu']['user'].append(round(float(cpu_stats.get('user', 0)), 2))
                            data['cpu']['system'].append(round(float(cpu_stats.get('system', cpu_stats.get('sys', 0))), 2))
                            data['cpu']['iowait'].append(round(float(cpu_stats.get('iowait', 0)), 2))
                            data['cpu']['idle'].append(round(float(cpu_stats.get('idle', 0)), 2))

                    elif metric_name == 'memory':
                        mem_stats = item.get('memory', {})
                        if mem_stats:
                            data['memory']['metrics']['used'].append(round(float(mem_stats.get('memused-percent', 0)), 2))
                            data['memory']['metrics']['buffer'].append(round(float(mem_stats.get('buffers', 0)), 2))
                            data['memory']['metrics']['cache'].append(round(float(mem_stats.get('cached', 0)), 2))
                            data['memory']['metrics']['commit'].append(round(float(mem_stats.get('commit-percent', 0)), 2))

                    elif metric_name == 'network':
                        for interface in item['network']['net-dev']:
                            iface = interface['iface']
                            if iface.startswith(('enp', 'ens', 'eth', 'br', 'eno', 'bond')):
                                if iface not in data['network']['metrics']['interfaces']:
                                    data['network']['metrics']['interfaces'].append(iface)
                                    data['network']['metrics']['rx_bytes'][iface] = []
                                    data['network']['metrics']['tx_bytes'][iface] = []

                                rx_mbps = float(interface['rxkB']) * 0.008
                                tx_mbps = float(interface['txkB']) * 0.008

                                data['network']['metrics']['rx_bytes'][iface].append(round(rx_mbps, 2))
                                data['network']['metrics']['tx_bytes'][iface].append(round(tx_mbps, 2))

                    elif metric_name == 'disk':
                        for disk in item['disk']:
                            device = disk['disk-device']
                            if device not in data['disk']['metrics']['devices']:
                                data['disk']['metrics']['devices'].append(device)
                                data['disk']['metrics']['read_mb'][device] = []
                                data['disk']['metrics']['write_mb'][device] = []
                                data['disk']['metrics']['util_percent'][device] = []

                            metrics = process_disk_metrics(disk, kernel_version)
                            data['disk']['metrics']['read_mb'][device].append(round(metrics['read_mb'], 2))
                            data['disk']['metrics']['write_mb'][device].append(round(metrics['write_mb'], 2))
                            data['disk']['metrics']['util_percent'][device].append(round(metrics['util_percent'], 2))

                    elif metric_name == 'load':
                        queue = item.get('queue', {})
                        data['load']['metrics']['ldavg_1'].append(round(float(queue.get('ldavg-1', 0)), 2))
                        data['load']['metrics']['ldavg_5'].append(round(float(queue.get('ldavg-5', 0)), 2))
                        data['load']['metrics']['ldavg_15'].append(round(float(queue.get('ldavg-15', 0)), 2))
                        data['load']['metrics']['plist_sz'].append(int(queue.get('plist-sz', 0)))

            except Exception as e:
                logger.error(f"Error processing {metric_name} data: {str(e)}")
                logger.error(traceback.format_exc())
                continue

        logger.info(f"Processed {len(data['timestamp'])} total data points")
        logger.debug(f"Final data structure: {json.dumps(data, default=str)}")

        # 최종 데이터 확인
        logger.info("Memory metrics summary:")
        logger.info(f"- Used: {len(data['memory']['metrics']['used'])} points")
        logger.info(f"- Buffer: {len(data['memory']['metrics']['buffer'])} points")
        logger.info(f"- Cache: {len(data['memory']['metrics']['cache'])} points")
        logger.info(f"- Commit: {len(data['memory']['metrics']['commit'])} points")
        if data['memory']['metrics']['commit']:
            logger.info(f"- Sample commit values: {data['memory']['metrics']['commit'][:5]}")

        # 결과 데이터에 메타데이터 추가
        data['metadata'] = metadata

        return data

    except Exception as e:
        logger.error(f"Error in process_sar_data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# Ctrl+C 인터럽트 처리 추가
def signal_handler(sig, frame):
    logger.info("Shutting down server...")
    cleanup_old_files()  # 종료 전 파일 정리
    sys.exit(0)

# 더 강력한 보안 헤더 추가
@app.after_request
def add_security_headers(response):
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    return response

# WSGI 미들웨어를 사용하여 /sar-service 접두사 추가
application = DispatcherMiddleware(NotFound(), {
    '/sar-service': app
})

# WSGI 서버가 application을 사용하도록 설정
if __name__ == '__main__':
    # Ctrl+C 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)

    # 클린업 스케줄러 시작
    cleanup_thread = threading.Thread(target=start_cleanup_scheduler)
    cleanup_thread.daemon = True
    cleanup_thread.start()

    # 서버 시작
    logger.info('Server starting on http://0.0.0.0:5000')
    try:
        from werkzeug.serving import run_simple
        run_simple('0.0.0.0', 5000, application, use_reloader=True)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        cleanup_old_files()
        sys.exit(0)
