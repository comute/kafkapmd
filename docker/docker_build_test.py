import subprocess
from datetime import date
import argparse
from distutils.dir_util import copy_tree
import shutil

def build_jvm(image, tag, kafka_url):
    image = f'{image}:{tag}'
    copy_tree("resources", "jvm/resources")
    result = subprocess.run(["docker", "build", "-f", "jvm/Dockerfile", "-t", image, "--build-arg", f"kafka_url={kafka_url}",
                            "--build-arg", f'build_date={date.today()}', "jvm"])
    if result.stderr:
        print(result.stdout)
        return
    shutil.rmtree("jvm/resources")

def run_jvm_tests(image, tag):
    subprocess.run(["python3", "docker_sanity_test.py", f"{image}:{tag}", "jvm"], cwd="test")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("image", help="Image name that you want to keep for the Docker image")
    parser.add_argument("-tag", "--image-tag", default="latest", dest="tag", help="Image tag that you want to add to the image")
    parser.add_argument("-type", "--image-type", default="all", dest="image_type", help="Image type you want to build. By default it's all")
    parser.add_argument("-u", "--kafka-url", dest="kafka_url", help="Kafka url to be used to download kafka binary tarball in the docker image")
    parser.add_argument("-b", "--build", action="store_true", dest="build_only", default=False, help="Only build the image, don't run tests")
    parser.add_argument("-t", "--test", action="store_true", dest="test_only", default=False, help="Only run the tests, don't build the image")
    args = parser.parse_args()

    if args.image_type in ("all", "jvm") and (args.build_only or not (args.build_only or args.test_only)):
        if args.kafka_url:
            build_jvm(args.image, args.tag, args.kafka_url)
        else:
            raise ValueError("--kafka-url is a required argument for jvm image")
    
    if args.image_type in ("all", "jvm") and (args.test_only or not (args.build_only or args.test_only)):
        run_jvm_tests(args.image, args.tag)