sudo apt-get update
sudo apt-get -y install python3 python3-pip git
git clone https://github.com/Maciekkucharski/BGT_LAB_4.git
pip install dask[complete]
pip install pyarrow

cd BGT_LAB_4
mkdir ~/data1
gsutil -m cp gs://pjwstk-bigdata/*.parquet ./data1/.
sudo cp -r data1 data2
sudo cp -r data1 data3
sudo cp -r data1 data4
export PATH="/home/s20153/.local/bin:$PATH"