import os
import json
import shutil
import subprocess
import threading
import time
import uuid
import zipfile
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

from pdf_parser_cli import process_pdf_folder


BASE_DIR = Path(__file__).resolve().parent
WORK_DIR = BASE_DIR / "web_jobs"
JOB_DIR = WORK_DIR / "jobs"
UPLOAD_DIR = WORK_DIR / "uploads"
EXTRACT_DIR = WORK_DIR / "extracted"
OUTPUT_DIR = WORK_DIR / "outputs"
ALLOWED_EXTENSIONS = {".zip", ".rar"}
PDF_EXTENSION = ".pdf"


app = Flask(__name__)

for folder in (WORK_DIR, JOB_DIR, UPLOAD_DIR, EXTRACT_DIR, OUTPUT_DIR):
    folder.mkdir(parents=True, exist_ok=True)

jobs = {}
jobs_lock = threading.Lock()


def allowed_file(filename):
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


def allowed_pdf_file(filename):
    return Path(filename).suffix.lower() == PDF_EXTENSION


def job_file_path(job_id):
    return JOB_DIR / f"{job_id}.json"


def read_job(job_id):
    path = job_file_path(job_id)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    with jobs_lock:
        return jobs.get(job_id)


def write_job(job):
    path = job_file_path(job["job_id"])
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(job, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def list_jobs(limit=10):
    records = []
    for path in sorted(JOB_DIR.glob("*.json"), reverse=True):
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            continue
    records.sort(key=lambda item: item.get("created_at", 0), reverse=True)
    return records[:limit]


def update_job(job_id, **kwargs):
    with jobs_lock:
        job = jobs.get(job_id) or read_job(job_id) or {"job_id": job_id}
        job.update(kwargs)
        jobs[job_id] = job
        write_job(job)


def append_job_error(job_id, filename, stage, reason):
    with jobs_lock:
        job = jobs.get(job_id) or read_job(job_id) or {"job_id": job_id}
        job.setdefault("errors", []).append({
            "文件名": filename,
            "阶段": stage,
            "原因": reason,
        })
        jobs[job_id] = job
        write_job(job)


def extract_archive(archive_path, destination_dir):
    suffix = archive_path.suffix.lower()
    if suffix == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(destination_dir)
        return

    if suffix == ".rar":
        result = subprocess.run(
            ["bsdtar", "-xf", str(archive_path), "-C", str(destination_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise ValueError(f"RAR 解压失败: {stderr or 'bsdtar returned non-zero exit status'}")
        return

    raise ValueError("仅支持 zip 或 rar 压缩包")


def find_pdf_root(extract_root):
    pdf_files = list(extract_root.rglob("*.pdf"))
    if not pdf_files:
        raise ValueError("压缩包中没有找到 PDF 文件")

    root_candidates = {pdf.parent for pdf in pdf_files}
    if len(root_candidates) == 1:
        return next(iter(root_candidates))
    return extract_root


def run_job(job_id, archive_path=None, pdf_root=None):
    extract_root = EXTRACT_DIR / job_id
    output_file = OUTPUT_DIR / f"{job_id}.xlsx"

    try:
        if archive_path:
            update_job(job_id, stage="extracting", message="正在解压压缩包")
            extract_archive(archive_path, extract_root)
            pdf_root = find_pdf_root(extract_root)
        else:
            if not pdf_root or not Path(pdf_root).exists():
                raise ValueError("未找到可处理的 PDF 文件")
            update_job(job_id, stage="processing", message="正在解析 PDF")

        update_job(job_id, stage="processing", message="正在解析 PDF")

        def on_progress(payload):
            stage = payload.get("stage")
            if stage == "starting":
                update_job(
                    job_id,
                    stage="processing",
                    total_files=payload.get("total_files", 0),
                    processed_files=payload.get("processed_files", 0),
                    success_count=payload.get("success_count", 0),
                    failure_count=payload.get("failure_count", 0),
                    skipped_count=payload.get("skipped_count", 0),
                    current_file="",
                    message="开始解析 PDF",
                    skipped=payload.get("skipped", []),
                )
            elif stage == "processing":
                last_result = payload.get("last_result", {})
                update_job(
                    job_id,
                    stage="processing",
                    total_files=payload.get("total_files", 0),
                    processed_files=payload.get("processed_files", 0),
                    success_count=payload.get("success_count", 0),
                    failure_count=payload.get("failure_count", 0),
                    skipped_count=payload.get("skipped_count", 0),
                    current_file=payload.get("current_file", ""),
                    last_result=last_result,
                    errors=payload.get("errors", []),
                    skipped=payload.get("skipped", []),
                    message=f"正在处理 {payload.get('current_file', '')}",
                )
            elif stage == "completed":
                update_job(
                    job_id,
                    stage="completed",
                    total_files=payload.get("total_files", 0),
                    processed_files=payload.get("total_files", 0),
                    success_count=payload.get("success_count", 0),
                    failure_count=payload.get("failure_count", 0),
                    skipped_count=payload.get("skipped_count", 0),
                    total_rows=payload.get("total_rows", 0),
                    current_file="",
                    errors=payload.get("errors", []),
                    skipped=payload.get("skipped", []),
                    output_file=str(output_file),
                    download_url=f"/api/jobs/{job_id}/download",
                    message="处理完成",
                )

        process_pdf_folder(str(pdf_root), str(output_file), progress_callback=on_progress)
    except Exception as exc:
        append_job_error(job_id, "", "job", str(exc))
        current_job = read_job(job_id) or {}
        update_job(
            job_id,
            stage="failed",
            message=str(exc),
            current_file="",
            failure_count=current_job.get("failure_count", 0) or 1,
        )


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/api/jobs")
def create_job():
    upload = request.files.get("archive")
    pdf_uploads = [file for file in request.files.getlist("pdf_files") if file and file.filename]
    has_archive = bool(upload and upload.filename)
    has_pdfs = bool(pdf_uploads)

    if not has_archive and not has_pdfs:
        return jsonify({"error": "请先选择 zip/rar 压缩包，或直接选择多个 PDF 文件"}), 400

    if has_archive and has_pdfs:
        return jsonify({"error": "压缩包上传和多 PDF 上传请二选一"}), 400

    job_id = uuid.uuid4().hex
    archive_path = None
    pdf_input_dir = None
    display_filename = ""

    if has_archive:
        if not allowed_file(upload.filename):
            return jsonify({"error": "压缩包仅支持 zip 或 rar"}), 400
        ext = Path(upload.filename).suffix.lower()
        filename = secure_filename(Path(upload.filename).stem) or f"upload_{job_id}"
        archive_path = UPLOAD_DIR / f"{job_id}_{filename}{ext}"
        upload.save(archive_path)
        display_filename = upload.filename
        source_type = "archive"
    else:
        invalid_files = [file.filename for file in pdf_uploads if not allowed_pdf_file(file.filename)]
        if invalid_files:
            return jsonify({"error": f"以下文件不是 PDF: {', '.join(invalid_files[:5])}"}), 400

        pdf_input_dir = EXTRACT_DIR / job_id / "direct_upload"
        pdf_input_dir.mkdir(parents=True, exist_ok=True)
        seen_names = set()
        for index, file in enumerate(pdf_uploads, 1):
            original_name = Path(file.filename).name
            safe_name = secure_filename(Path(original_name).stem) or f"pdf_{index}"
            final_name = f"{safe_name}.pdf"
            while final_name in seen_names:
                final_name = f"{safe_name}_{index}.pdf"
            seen_names.add(final_name)
            file.save(pdf_input_dir / final_name)

        display_filename = f"已选择 {len(pdf_uploads)} 个 PDF 文件"
        source_type = "pdf_files"

    job = {
        "job_id": job_id,
        "stage": "queued",
        "message": "任务已创建，等待开始",
        "filename": display_filename,
        "source_type": source_type,
        "created_at": int(time.time()),
        "total_files": 0,
        "processed_files": 0,
        "success_count": 0,
        "failure_count": 0,
        "skipped_count": 0,
        "total_rows": 0,
        "current_file": "",
        "errors": [],
        "skipped": [],
        "download_url": "",
    }
    with jobs_lock:
        jobs[job_id] = job
        write_job(job)

    thread = threading.Thread(
        target=run_job,
        kwargs={
            "job_id": job_id,
            "archive_path": archive_path,
            "pdf_root": str(pdf_input_dir) if pdf_input_dir else None,
        },
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id})


@app.get("/api/jobs/<job_id>")
def get_job(job_id):
    job = read_job(job_id)
    if not job:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify(job)


@app.get("/api/jobs")
def get_jobs():
    limit = request.args.get("limit", default=10, type=int)
    limit = max(1, min(limit, 50))
    return jsonify({"jobs": list_jobs(limit=limit)})


@app.get("/api/jobs/<job_id>/download")
def download_job_output(job_id):
    job = read_job(job_id)
    if not job:
        return jsonify({"error": "任务不存在"}), 404
    output_file = job.get("output_file")
    if job.get("stage") != "completed" or not output_file or not os.path.exists(output_file):
        return jsonify({"error": "文件尚未生成"}), 400

    return send_file(
        output_file,
        as_attachment=True,
        download_name=f"amazon_summary_{job_id}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
